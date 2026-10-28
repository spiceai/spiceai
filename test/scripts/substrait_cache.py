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

"""Black-box serialized-plan cache contract over Flight SQL.

uv run --python 3.12 --with datafusion==55.0.0 --with pyarrow==26.0.0 \
  python test/scripts/substrait_cache.py --spiced target/release/spiced --out /tmp/substrait-cache
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from datafusion import SessionContext
from datafusion.substrait import Producer
import pyarrow.flight as flight

from sidecar import Instance, base_pod


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, payload):
    return varint((number << 3) | 2) + varint(len(payload)) + payload


def command(plan):
    # Standard google.protobuf.Any containing the Flight SQL command. Encoding
    # it independently avoids sharing the Spice command construction code.
    substrait = field(1, plan)
    message = field(1, substrait)
    return field(1, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementSubstraitPlan") + field(2, message)


class Headers(flight.ClientMiddleware):
    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        self.factory.headers.append(headers)


class Capture(flight.ClientMiddlewareFactory):
    def __init__(self):
        self.headers = []

    def start_call(self, info):
        return Headers(self)


def execute(client, capture, plan, options):
    capture.headers = []
    actual = client.do_get(flight.Ticket(command(plan)), options=options).read_all().to_pylist()
    status = None
    for headers in capture.headers:
        values = headers.get("results-cache-status")
        if values:
            status = values[0]
            if isinstance(status, bytes):
                status = status.decode()
    return actual, status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    args.spiced = args.spiced.resolve()
    ctx = SessionContext()
    plan = bytes(Producer.to_substrait_plan(ctx.sql("SELECT 11 AS value").optimized_logical_plan(), ctx).encode())
    # A protobuf unknown field changes wire bytes but not decoded operations.
    # It distinguishes byte identity from logical-plan identity without changing
    # the expected answer. Prost must ignore this field during plan decoding.
    equivalent = plan + varint(1000 << 3) + varint(1)
    (args.out / "plan.bin").write_bytes(plan)
    (args.out / "equivalent-plan.bin").write_bytes(equivalent)
    (args.out / "provenance.json").write_text(json.dumps({
        "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "binary_sha256": hashlib.sha256(args.spiced.read_bytes()).hexdigest(),
        "plan_sha256": hashlib.sha256(plan).hexdigest(),
        "equivalent_plan_sha256": hashlib.sha256(equivalent).hexdigest(),
    }, indent=2))
    observations = []
    mismatches = []
    for key_type in ["plan", "sql"]:
        pod = base_pod("substrait-cache", [], cache=True)
        pod["runtime"]["caching"]["sql_results"]["cache_key_type"] = key_type
        pod["runtime"]["auth"] = {"api-key": {"enabled": True, "keys": ["alice-cache-key", "bob-cache-key:ro"]}}
        instance = Instance(args.spiced, args.out / key_type, pod, api_key="alice-cache-key")
        try:
            instance.start()
            capture = Capture()
            client = flight.FlightClient(f"grpc://127.0.0.1:{instance.flight}", middleware=[capture])
            alice = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "alice-cache-key")])
            bob = flight.FlightCallOptions(headers=[client.authenticate_basic_token("", "bob-cache-key")])
            expected_statuses = ["MISS", "HIT", "MISS", "HIT", "MISS", "HIT"] if key_type == "plan" else [None] * 6
            requests = [(plan, alice), (plan, alice), (equivalent, alice), (equivalent, alice), (plan, bob), (plan, bob)]
            for number, ((wire, options), expected_status) in enumerate(zip(requests, expected_statuses)):
                rows, status = execute(client, capture, wire, options)
                observation = {"mode": key_type, "request": number, "status": status, "expected_status": expected_status, "rows": rows}
                observations.append(observation)
                (args.out / "observations.json").write_text(json.dumps(observations, indent=2))
                print(json.dumps(observation), flush=True)
                assert rows == [{"value": 11}], observation
                if status != expected_status:
                    mismatches.append(observation)
            client.close()
        finally:
            instance.stop()
    assert not mismatches, mismatches


if __name__ == "__main__":
    main()
