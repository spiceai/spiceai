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

"""Reproduce sidecar semantic regressions against SQLite and literal oracles.

Runs unchanged against each candidate binary, preserving every wrong answer,
error, plan and source identity before exiting nonzero on any mismatch.
"""

import argparse
import hashlib
import json
import subprocess
import time
import urllib.error
from pathlib import Path

from sidecar import Instance, base_pod, expected, fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out = args.out.resolve()
    args.spiced = args.spiced.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / "provenance.json").write_text(
        json.dumps(
            {
                "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "commit": args.source_commit,
                "harness_commit": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
                "harness_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "binary_sha256": hashlib.sha256(args.spiced.read_bytes()).hexdigest(),
                "arguments": {key: str(value) for key, value in vars(args).items()},
            },
            indent=2,
        )
    )
    database, source = fixture(args.out, 20)
    pod = base_pod(
        "semantics-origin",
        [
            {
                "from": f"file:{source}",
                "name": "remote_orders",
                "params": {"file_format": "csv"},
            }
        ],
    )
    pod["runtime"]["functions"] = {"enabled": True}
    pod["functions"] = [
        {
            "name": name,
            "from": "sql",
            "signature": {"args": arguments, "returns": returns},
            "body": body,
            "volatility": "immutable",
            "as_tool": False,
        }
        for name, arguments, returns, body in [
            (
                "add",
                [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}],
                "int64",
                "a - b",
            ),
            ("is_nan", [{"name": "x", "type": "float64"}], "boolean", "x > -100"),
        ]
    ]
    upstream = Instance(args.spiced, args.out / "upstream", pod)
    sidecar = Instance(
        args.spiced,
        args.out / "sidecar",
        base_pod(
            "semantics-sidecar",
            [
                {
                    "from": "spice.ai:remote_orders",
                    "name": "orders",
                    "params": {
                        "spiceai_endpoint": f"http://127.0.0.1:{upstream.flight}"
                    },
                }
            ],
            "sidecar",
        ),
    )
    results = []
    try:
        upstream.start()
        sidecar.start()
        controls, _ = upstream.query(
            "SELECT add(CAST(4 AS BIGINT), CAST(1 AS BIGINT)) AS arithmetic, is_nan(CAST(1 AS DOUBLE)) AS alias"
        )
        assert controls == [{"arithmetic": 3, "alias": True}], controls
        queries = [
            (
                "null-aware-antijoin",
                "SELECT id FROM orders WHERE value NOT IN (SELECT value FROM orders) ORDER BY id",
                None,
            ),
            (
                "operator-identity",
                "SELECT id + CAST(1 AS BIGINT) AS n FROM orders WHERE id < 5 ORDER BY id",
                None,
            ),
            (
                "builtin-identity",
                "SELECT id, isnan(CAST(id AS DOUBLE)) AS n FROM orders WHERE id < 10 ORDER BY id",
                [{"id": i, "n": False} for i in range(10)],
            ),
            (
                "builtin-argument-order",
                "SELECT id, log(CAST(2 AS DOUBLE), CAST(id AS DOUBLE)) AS n FROM orders WHERE id = 8",
                [{"id": 8, "n": 3.0}],
            ),
            (
                "flight-string-view",
                "SELECT arrow_cast(label, 'Utf8View') AS label FROM orders WHERE id < 5 ORDER BY id",
                expected(database, "SELECT label FROM orders WHERE id < 5 ORDER BY id"),
            ),
        ]
        for name, sql, wanted in queries:
            if wanted is None:
                wanted = expected(database, sql)
            try:
                actual, _ = sidecar.query(sql)
            except urllib.error.HTTPError as error:
                actual = {"status": error.code, "error": error.read().decode()}
            explained, _ = sidecar.query("EXPLAIN " + sql)
            result = {
                "name": name,
                "sql": sql,
                "expected": wanted,
                "actual": actual,
                "passed": actual == wanted,
            }
            results.append(result)
            (args.out / f"{name}-explain.json").write_text(
                json.dumps(explained, indent=2)
            )
            print(json.dumps(result), flush=True)
        (args.out / "results.json").write_text(json.dumps(results, indent=2))
        assert all(result["passed"] for result in results), (
            "See results.json for semantic mismatches"
        )
    finally:
        sidecar.stop()
        upstream.stop()
        database.close()


if __name__ == "__main__":
    main()
