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

"""Typed native sidecar round trips with literal Python/Arrow oracles.

uv run --python 3.12 --with pyarrow==26.0.0 python test/scripts/sidecar_types.py \
  --spiced target/release/spiced --out /tmp/sidecar-types
"""

import argparse
import hashlib
import json
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import flight
from sidecar import Instance, base_pod, field

INPUT = [
    {"id": 1, "value": 11, "label": "雪"},
    {"id": 2, "value": None, "label": ""},
    {"id": 3, "value": -7, "label": None},
    {"id": 4, "value": 0, "label": "x"},
]


def query(client, sql):
    command = field(1, sql.encode())
    ticket = field(
        1, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    ) + field(2, command)
    return client.do_get(flight.Ticket(ticket)).read_all()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--source-commit",
        help="Commit that produced the prebuilt spiced binary; defaults to HEAD",
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["local", "server", "sidecar"],
        default=["local", "server", "sidecar"],
    )
    args = parser.parse_args()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    args.spiced = args.spiced.resolve()
    (args.out / "provenance.json").write_text(
        json.dumps(
            {
                "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "commit": args.source_commit
                or subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
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
            },
            indent=2,
        )
    )
    source = args.out / "typed.parquet"
    schema = pa.schema(
        [("id", pa.int64()), ("value", pa.int64()), ("label", pa.string())]
    )
    pq.write_table(pa.Table.from_pylist(INPUT, schema=schema), source)
    upstream = Instance(
        args.spiced,
        args.out / "upstream",
        base_pod(
            "typed-upstream",
            [
                {"from": f"file:{source}", "name": "remote_orders"},
            ],
        ),
    )
    upstream.start()
    datasets = [
        {
            "from": f"{prefix}:remote_orders",
            "name": name,
            "params": {"spiceai_endpoint": f"http://127.0.0.1:{upstream.flight}"},
        }
        for prefix, name in [("spice.ai", "orders"), ("spiceai", "other_orders")]
    ]
    cases = [
        ("rows", "SELECT id, value, label FROM orders ORDER BY id", INPUT),
        (
            "decimal",
            "SELECT CAST(value AS DECIMAL(20, 3)) AS v FROM orders ORDER BY id",
            [
                {
                    "v": None
                    if row["value"] is None
                    else Decimal(row["value"]).quantize(Decimal(".001"))
                }
                for row in INPUT
            ],
        ),
        (
            "list",
            "SELECT make_array(id, value) AS v FROM orders ORDER BY id",
            [{"v": [row["id"], row["value"]]} for row in INPUT],
        ),
        (
            "struct",
            "SELECT named_struct('key', id, 'value', label) AS v FROM orders ORDER BY id",
            [{"v": {"key": row["id"], "value": row["label"]}} for row in INPUT],
        ),
        (
            "map",
            "SELECT map(make_array('key', 'value'), make_array(id, value)) AS v FROM orders ORDER BY id",
            [{"v": [("key", row["id"]), ("value", row["value"])]} for row in INPUT],
        ),
        (
            "timestamp",
            "SELECT to_timestamp_seconds(id) AS v FROM orders ORDER BY id",
            [
                {"v": datetime(1970, 1, 1) + timedelta(seconds=row["id"])}
                for row in INPUT
            ],
        ),
        (
            "binary",
            "SELECT arrow_cast(label, 'Binary') AS v FROM orders ORDER BY id",
            [
                {"v": None if row["label"] is None else row["label"].encode()}
                for row in INPUT
            ],
        ),
        (
            "view",
            "SELECT arrow_cast(label, 'Utf8View') AS v FROM orders ORDER BY id",
            [{"v": row["label"]} for row in INPUT],
        ),
        (
            "empty-decimal",
            "SELECT CAST(value AS DECIMAL(20, 3)) AS v FROM orders WHERE id < 0",
            [],
        ),
    ]
    failures = []
    try:
        for mode in args.modes:
            configured = (
                datasets
                if mode != "local"
                else [{"from": f"file:{source}", "name": "orders"}]
            )
            instance = Instance(
                args.spiced,
                args.out / mode,
                base_pod(mode, configured, None if mode == "local" else mode),
            )
            client = None
            try:
                instance.start()
                client = flight.FlightClient(f"grpc://127.0.0.1:{instance.flight}")
                results = []
                for name, sql, oracle in cases:
                    try:
                        actual = query(client, sql)
                    except (pa.ArrowInvalid, flight.FlightError) as error:
                        result = {
                            "name": name,
                            "sql": sql,
                            "error": str(error),
                            "expected": oracle,
                        }
                        # Preserve the SQL federation errors observed on the
                        # source baseline as explicit negative controls.
                        expected_unsupported = mode == "server" and (
                            (name in {"struct", "map"} and "found: {" in str(error))
                            or (
                                name == "binary"
                                and "Unknown command type: " in str(error)
                            )
                        )
                        result["expected_unsupported"] = expected_unsupported
                        results.append(result)
                        if not expected_unsupported:
                            failures.append({"mode": mode, **result})
                        print(
                            f"{mode}: {name} {'expected SQL federation limitation' if expected_unsupported else 'FAILED'}",
                            flush=True,
                        )
                    else:
                        result = {
                            "name": name,
                            "sql": sql,
                            "actual": actual.to_pylist(),
                            "expected": oracle,
                            "schema": str(actual.schema),
                        }
                        results.append(result)
                        matches = actual.to_pylist() == oracle
                        if "decimal" in name:
                            matches = matches and actual.schema.field(
                                "v"
                            ).type == pa.decimal128(20, 3)
                        if not matches:
                            failures.append({"mode": mode, **result})
                        print(
                            f"{mode}: {name} {'exact typed rows' if matches else 'FAILED'}",
                            flush=True,
                        )
                    (instance.directory / "results.json").write_text(
                        json.dumps(results, indent=2, default=str)
                    )
                successful = {
                    result["name"] for result in results if "actual" in result
                }
                concurrent_cases = [case for case in cases if case[0] in successful]
                barrier = threading.Barrier(32)

                def validate_worker(
                    worker_id,
                    source_client=client,
                    start_gate=barrier,
                    comparisons=concurrent_cases,
                ):
                    start_gate.wait(timeout=10)
                    observed = []
                    for iteration in range(10):
                        name, sql, oracle = comparisons[
                            (worker_id + iteration) % len(comparisons)
                        ]
                        record = {
                            "worker": worker_id,
                            "iteration": iteration,
                            "name": name,
                            "expected": oracle,
                            "started_ns": time.perf_counter_ns(),
                        }
                        try:
                            actual = query(source_client, sql)
                        except (pa.ArrowInvalid, flight.FlightError) as error:
                            record["error"] = str(error)
                        else:
                            record["actual"] = actual.to_pylist()
                            record["schema"] = str(actual.schema)
                        record["finished_ns"] = time.perf_counter_ns()
                        observed.append(record)
                    return observed

                concurrent_results = []
                with ThreadPoolExecutor(max_workers=32) as pool:
                    for records in pool.map(validate_worker, range(32)):
                        concurrent_results.extend(records)
                (instance.directory / "concurrent-results.json").write_text(
                    json.dumps(concurrent_results, indent=2, default=str)
                )
                mismatches = [
                    record
                    for record in concurrent_results
                    if record.get("actual") != record["expected"]
                ]
                failures.extend({"mode": mode, **record} for record in mismatches)
                print(
                    f"{mode}: {len(concurrent_results)} typed comparisons with 32 client threads; {len(mismatches)} mismatches",
                    flush=True,
                )

                for name, sql in [
                    (
                        "cast-error",
                        "SELECT CAST(id * 1000000 AS TINYINT) AS v FROM orders",
                    ),
                    (
                        "divide-error",
                        "SELECT 10 / (id - 1) AS v FROM orders WHERE id = 1",
                    ),
                ]:
                    try:
                        query(client, sql)
                    except (pa.ArrowInvalid, flight.FlightError) as error:
                        result = {"name": name, "sql": sql, "error": str(error)}
                        assert any(
                            word in str(error).lower()
                            for word in ["cast", "overflow", "divide", "division"]
                        ), result
                        results.append(result)
                        (instance.directory / "results.json").write_text(
                            json.dumps(results, indent=2, default=str)
                        )
                        print(f"{mode}: {name} returned an error", flush=True)
                    else:
                        raise AssertionError(f"{mode}: {name} returned rows")
            finally:
                if client:
                    client.close()
                instance.stop()
    finally:
        upstream.stop()
    assert not failures, failures


if __name__ == "__main__":
    main()
