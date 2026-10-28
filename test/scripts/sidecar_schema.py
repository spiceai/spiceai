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

"""Check cached sidecar bindings across upstream schema replacement and recovery."""

import argparse
import csv
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
    args.spiced = args.spiced.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / "provenance.json").write_text(
        json.dumps(
            {
                "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "commit": args.source_commit,
                "binary_sha256": hashlib.sha256(args.spiced.read_bytes()).hexdigest(),
                "harness_commit": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
                "harness_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "common_harness_sha256": hashlib.sha256(
                    Path(__file__).with_name("sidecar.py").read_bytes()
                ).hexdigest(),
                "arguments": {key: str(value) for key, value in vars(args).items()},
            },
            indent=2,
        )
    )
    database, source = fixture(args.out, 20)
    pod = base_pod(
        "schema-origin",
        [
            {
                "from": f"file:{source}",
                "name": "remote_orders",
                "params": {"file_format": "csv"},
            }
        ],
    )
    upstream = Instance(
        args.spiced, args.out / "upstream", pod, extra_args=["--pods-watcher-enabled"]
    )
    client = Instance(
        args.spiced,
        args.out / "sidecar",
        base_pod(
            "schema-client",
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
    sql = "SELECT id, value FROM orders WHERE id < 3 ORDER BY id"
    wanted = expected(database, sql)
    events = []

    def fresh(sql):
        return upstream.query(
            sql + f" /* {time.monotonic_ns()} */", {"Cache-Control": "no-cache"}
        )[0]

    def replace(path, ready_sql, ready_rows):
        pod["datasets"][0]["from"] = f"file:{path}"
        upstream.pod.write_text(json.dumps(pod, indent=2))
        limit = time.monotonic() + 30
        observations = []
        while True:
            try:
                actual = fresh(ready_sql)
            except urllib.error.HTTPError as error:
                actual = {"status": error.code, "error": error.read().decode()}
            observations.append(actual)
            if actual == ready_rows:
                return observations
            assert time.monotonic() < limit, observations
            time.sleep(0.1)

    try:
        upstream.start()
        client.start()
        assert client.query(sql)[0] == wanted
        reordered = args.out / "reordered.csv"
        with reordered.open("w", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["value", "id", "label"])
            writer.writerows(
                (None if i % 7 == 0 else i * 3 - 50, i, f"group{i % 10}")
                for i in range(20)
            )
        observations = replace(
            reordered,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'remote_orders' ORDER BY ordinal_position",
            [{"column_name": name} for name in ["value", "id", "label"]],
        )
        control = fresh(sql.replace("orders", "remote_orders"))
        actual = client.query(sql)[0]
        events.append(
            {
                "stage": "same-type-reorder",
                "upstream_observations": observations,
                "control": control,
                "actual": actual,
                "expected": wanted,
            }
        )
        (args.out / "results.json").write_text(json.dumps(events, indent=2))
        assert control == actual == wanted, events[-1]
        print(
            "same-type column reorder: exact SQLite rows with cached sidecar schema",
            flush=True,
        )

        incompatible = args.out / "incompatible.csv"
        with incompatible.open("w", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["value", "id", "label"])
            writer.writerows(("not-an-integer", i, f"group{i % 10}") for i in range(20))
        observations = replace(
            incompatible,
            "SELECT value FROM remote_orders WHERE id = 1",
            [{"value": "not-an-integer"}],
        )
        try:
            actual = client.query(sql)[0]
        except urllib.error.HTTPError as error:
            actual = {"status": error.code, "error": error.read().decode()}
        events.append(
            {
                "stage": "incompatible-type",
                "upstream_observations": observations,
                "actual": actual,
            }
        )
        (args.out / "results.json").write_text(json.dumps(events, indent=2))
        assert isinstance(actual, dict) and actual.get("status") == 400, actual
        assert (
            "schema" in actual["error"].lower() or "type" in actual["error"].lower()
        ), actual
        print("incompatible type replacement: HTTP 400, no coerced rows", flush=True)

        observations = replace(
            source, "SELECT value FROM remote_orders WHERE id = 1", [{"value": -47}]
        )
        actual = client.query(sql)[0]
        events.append(
            {
                "stage": "schema-recovery",
                "upstream_observations": observations,
                "actual": actual,
                "expected": wanted,
            }
        )
        (args.out / "results.json").write_text(json.dumps(events, indent=2))
        assert actual == wanted, actual
        print("schema recovery: exact rows without restarting sidecar", flush=True)
    finally:
        client.stop()
        upstream.stop()
        database.close()


if __name__ == "__main__":
    main()
