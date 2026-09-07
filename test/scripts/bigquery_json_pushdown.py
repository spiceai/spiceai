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

"""Compare JSON pushdown with local evaluation through a real spiced and BigQuery.

Requires BIGQUERY_SERVICE_ACCOUNT_JSON_FILE, ADBC_BIGQUERY_DRIVER_PATH and
SPICED_BIN, using the same contract as bigquery_pushdown.py. Run with:

    uv run --with google-cloud-bigquery==3.38.0 test/scripts/bigquery_json_pushdown.py

Each run creates and deletes its own synthetic dataset. Results, plans, binary
identity, generated SQL and dataset-scoped execution jobs remain in
BIGQUERY_TEST_OUTPUT (defaults to target/bigquery-json-evidence/<timestamp>).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

import bigquery_pushdown as harness

NODES = [
    "null", '"say \\"hi\\""', "false", "42", "1.50", "1e+00", "-0.0",
    "9223372036854775807", "9223372036854775808", "-9223372036854775808",
    "-9223372036854775809", '"9223372036854775808"', '{"x": 1.50}', '[1.50, "x"]',
]
DOCS = [None, "{}"] + [
    '{"value":' + node + ',"context":{"iteration":' + node + '}}'
    for node in NODES
] + [
    '{"action_name":"PAYMENT.alpha","status":"pending","threshold":0.575}',
    '{"action_name":"PAYMENT.alpha","status":"complete","threshold":0.5750}',
    '{"action_name":"PAYMENT.beta","status":"complete","threshold":"0.575"}',
    '{"action_name":"IGNORED.beta","status":"pending","threshold":0.575}',
]
SCALAR_TEXT_IDS = [
    index for index, doc in enumerate(DOCS)
    if not isinstance(json.loads(doc or "{}").get("value"), (dict, list))
]
QUERIES = {
    "text-values": """SELECT id,
json_as_text(text_doc, 'value') AS text_value
FROM {table} WHERE scalar_text ORDER BY id""",
    "text-threshold": """SELECT id FROM {table}
WHERE json_as_text(text_doc, 'threshold') = '0.575' ORDER BY id""",
    "text-window": """SELECT id,
SUBSTR(json_as_text(text_doc, 'action_name'), 9) AS token,
json_as_text(text_doc, 'status') AS status,
ROW_NUMBER() OVER (
  PARTITION BY SUBSTR(json_as_text(text_doc, 'action_name'), 9)
  ORDER BY id DESC
) AS row_num
FROM {table}
WHERE json_as_text(text_doc, 'action_name') LIKE 'PAYMENT.%'
ORDER BY id""",
    "nested-null": """SELECT id,
json_get(text_doc, 'context', 'iteration') IS NULL AS text_null
FROM {table} ORDER BY id""",
    "native-document-control": """SELECT id,
json_as_text(native_doc, 'value') AS native_value,
json_get(native_doc, 'context', 'iteration') IS NULL AS native_null
FROM {table} ORDER BY id""",
    "coalesced-documents": """SELECT id,
json_as_text(COALESCE(text_doc, backup_text), 'value') AS text_value,
json_get(COALESCE(text_doc, backup_text), 'context', 'iteration') IS NULL AS text_null
FROM {table} WHERE scalar_text ORDER BY id""",
}


def main():
    info, credential = harness.credential_info()
    client = bigquery.Client(
        project=info["project_id"],
        credentials=service_account.Credentials.from_service_account_info(info),
    )
    driver = harness.required_path("ADBC_BIGQUERY_DRIVER_PATH")
    binary = harness.required_path("SPICED_BIN", harness.DEFAULT_SPICED)
    stamp = harness.utc_stamp()
    output = Path(os.environ.get(
        "BIGQUERY_TEST_OUTPUT", harness.ROOT / "target" / "bigquery-json-evidence" / stamp,
    )).resolve()
    output.mkdir(parents=True, exist_ok=False)
    dataset = bigquery.Dataset(f"{client.project}.spice_bq_json_{stamp.lower()}_{os.getpid()}")
    dataset.location = os.environ.get("BIGQUERY_LOCATION", "US")
    dataset.default_table_expiration_ms = 86_400_000
    dataset.labels = {"purpose": "spice-bigquery-json"}
    client.create_dataset(dataset)
    process = None
    records = []
    try:
        table = f"{dataset.project}.{dataset.dataset_id}.documents"
        setup = f"""CREATE TABLE `{table}` AS
WITH source AS (SELECT id, doc AS text_doc FROM UNNEST(@docs) AS doc WITH OFFSET id)
SELECT *, id IN UNNEST(@scalar_ids) AS scalar_text,
 SAFE.PARSE_JSON(text_doc) AS native_doc,
 '{{"value":1.50,"context":{{"iteration":7}}}}' AS backup_text,
 JSON '{{"value":1.50,"context":{{"iteration":7}}}}' AS backup_native FROM source"""
        job = client.query(setup, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("docs", "STRING", DOCS),
                bigquery.ArrayQueryParameter("scalar_ids", "INT64", SCALAR_TEXT_IDS),
            ],
        ))
        job.result()
        unsupported_docs = [r'{"value":"\ud83d\ude00"}', r'{"value":{"x":"\u0061"}}']
        unsupported_setup = f"CREATE TABLE `{dataset.project}.{dataset.dataset_id}.unsupported_documents` AS SELECT id, doc AS text_doc FROM UNNEST(@docs) doc WITH OFFSET id"
        unsupported_job = client.query(unsupported_setup, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("docs", "STRING", unsupported_docs)],
        ))
        unsupported_job.result()
        harness.write_json(output / "fixture.json", {
            "docs": DOCS, "job_id": job.job_id,
            "unsupported_docs": unsupported_docs, "unsupported_job": unsupported_job.job_id,
        })
        (output / "setup.sql").write_text(setup)
        params = f"""      adbc_driver: bigquery
      adbc_driver_path: {driver}
      adbc_uri: bigquery:///{client.project}
      adbc_driver_options: adbc.bigquery.sql.auth_type=adbc.bigquery.sql.auth_type.json_credential_string;adbc.bigquery.sql.auth_credentials=${{secrets:BIGQUERY_SERVICE_ACCOUNT_JSON}}"""
        pod = "version: v1\nkind: Spicepod\nname: bigquery-json\nruntime:\n  caching:\n    sql_results:\n      enabled: false\ndatasets:\n"
        for name, source in [
            ("remote_documents", "documents"), ("local_documents", "documents"),
            ("remote_unsupported", "unsupported_documents"), ("local_unsupported", "unsupported_documents"),
        ]:
            pod += f"  - from: adbc:{dataset.dataset_id}.{source}\n    name: {name}\n    params:\n{params}\n"
            if name.startswith("local_"):
                pod += "      query_federation: disabled\n"
        pod_path = output / "spicepod.yaml"
        pod_path.write_text(pod)
        env = os.environ.copy()
        env["BIGQUERY_SERVICE_ACCOUNT_JSON"] = credential
        http, flight = harness.distinct_free_ports()
        with (output / "spiced.log").open("wb") as log:
            process = subprocess.Popen([
                str(binary), "--http", f"127.0.0.1:{http}", "--flight", f"127.0.0.1:{flight}",
                "--telemetry-enabled", "false", str(pod_path),
            ], cwd=output, env=env, stdout=log, stderr=subprocess.STDOUT)
            harness.write_json(output / "candidate.json", {
                "binary": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "pid": process.pid, "http": http, "flight": flight,
            })
            harness.wait_until_ready(process, http, 180)
            for name, query in QUERIES.items():
                results = {}
                for mode in ["local", "remote"]:
                    sql = query.format(table=f"{mode}_documents")
                    start = datetime.now(timezone.utc)
                    status, _, body = harness.http_sql(http, sql)
                    end = datetime.now(timezone.utc)
                    (output / f"{name}-{mode}.sql").write_text(sql)
                    (output / f"{name}-{mode}.body").write_text(body)
                    if status != 200:
                        raise harness.HarnessError(f"{name}/{mode}: HTTP {status}: {body}")
                    results[mode] = json.loads(body)
                    if mode == "local":
                        continue
                    status, _, explain = harness.http_sql(http, "EXPLAIN VERBOSE " + sql)
                    (output / f"{name}.explain.json").write_text(explain)
                    if status != 200:
                        raise harness.HarnessError(f"{name}: EXPLAIN failed: {explain}")
                    pushed = harness.initial_physical_sql(explain)
                    (output / f"{name}.generated.sql").write_text(pushed)
                    expected = "SAFE_CAST" if name == "nested-null" else "JSON_VALUE"
                    has_rendering = expected in pushed
                    if name == "native-document-control":
                        has_rendering = "JSON_VALUE" not in pushed and "JSON_QUERY" not in pushed
                    if harness.pushed_statement_count(explain) != 1 or not has_rendering:
                        raise harness.HarnessError(f"{name}: JSON expression did not federate: {pushed}")
                    if "PARSE_JSON" in pushed or "json_get(" in pushed or "json_as_text(" in pushed:
                        raise harness.HarnessError(f"{name}: unsupported JSON rendering: {pushed}")
                    # Dataset ownership and request boundaries exclude setup,
                    # schema discovery, local-oracle scans and concurrent lanes.
                    jobs = []
                    deadline = time.monotonic() + 30
                    while not jobs:
                        jobs = [j for j in client.list_jobs(min_creation_time=start)
                                if j.created <= end and getattr(j, "query", None)
                                and dataset.dataset_id in j.query]
                        if time.monotonic() >= deadline:
                            break
                        if not jobs:
                            time.sleep(1)
                    records.append({"name": name, "jobs": [
                        {"id": j.job_id, "query": j.query, "created": str(j.created), "error": j.error_result}
                        for j in jobs
                    ]})
                    harness.write_json(output / "jobs.json", records)
                    if len(jobs) != 1 or jobs[0].error_result:
                        raise harness.HarnessError(f"{name}: expected one successful execution job: {records[-1]}")
                if results["remote"] != results["local"]:
                    raise harness.HarnessError(f"{name}: pushed results differ from local evaluation; see {output}")
                if not results["remote"]:
                    raise harness.HarnessError(f"{name}: empty fixture result does not establish correctness")
                print(f"{name}: matching rows and one execution job", flush=True)
            for mode in ["local", "remote"]:
                sql = f"SELECT id, json_as_text(text_doc, 'value') AS value FROM {mode}_documents WHERE NOT scalar_text ORDER BY id"
                status, _, body = harness.http_sql(http, sql)
                (output / f"container-control-{mode}.body").write_text(body)
                if mode == "local" and (status != 200 or len(json.loads(body)) != 2):
                    raise harness.HarnessError("Container text local control failed")
                if mode == "remote" and (status == 200 or "query_federation" not in body):
                    raise harness.HarnessError("Container text pushdown must fail explicitly")
            print("container text: explicit error and working local fallback", flush=True)
            for expression in ["json_as_text(text_doc, 'value')", "json_get(text_doc, 'value') IS NULL"]:
                for mode in ["local", "remote"]:
                    sql = f"SELECT id, {expression} AS value FROM {mode}_unsupported ORDER BY id"
                    status, _, body = harness.http_sql(http, sql)
                    label = "null" if "IS NULL" in expression else "text"
                    (output / f"escape-control-{label}-{mode}.body").write_text(body)
                    if mode == "local" and (status != 200 or len(json.loads(body)) != 2):
                        raise harness.HarnessError("Unicode escape local control failed")
                    if mode == "remote" and (status == 200 or "query_federation" not in body):
                        raise harness.HarnessError("Unsupported escape pushdown must fail explicitly")
            print("unsupported escapes: explicit error and working local fallback", flush=True)
        return 0
    finally:
        if process is not None:
            harness.stop_spiced(process)
        client.delete_dataset(dataset.reference, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
