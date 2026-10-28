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

"""Real mTLS identities must remain separate federation scopes.

uv run --python 3.12 --with datafusion==55.0.0 --with pyarrow==26.0.0 \
  python test/scripts/sidecar_scope.py --spiced target/release/spiced --out /tmp/sidecar-scope

A certificate-authenticated Flight source returns different integer rows for
Alice and Bob. The SQL oracle is the literal pair (1, 2), independent of Spice.
"""

import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path

import pyarrow as pa
from datafusion import SessionContext
from datafusion.substrait import Consumer, Serde
from pyarrow import flight
from sidecar import Instance, base_pod


def fields(data):
    def integer(offset):
        value, shift = 0, 0
        while True:
            byte = data[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value, offset
            shift += 7

    result, offset = {}, 0
    while offset < len(data):
        tag, offset = integer(offset)
        if tag & 7 == 2:
            length, offset = integer(offset)
            result[tag >> 3] = data[offset : offset + length]
            offset += length
        elif tag & 7 == 0:
            _, offset = integer(offset)
        else:
            raise ValueError(f"unexpected protobuf wire type: {tag & 7}")
    return result


def pki(directory):
    def run(*arguments):
        subprocess.run(
            ["openssl", *arguments],
            cwd=directory,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    run(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        "-days",
        "1",
        "-subj",
        "/CN=Sidecar Test CA",
    )
    (directory / "server.ext").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"
    )
    (directory / "client.ext").write_text("extendedKeyUsage=clientAuth\n")
    for name in ["server", "alice", "bob"]:
        run(
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
            "-subj",
            f"/CN={name}",
        )
        run(
            "x509",
            "-req",
            "-in",
            f"{name}.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            f"{name}.crt",
            "-days",
            "1",
            "-extfile",
            "server.ext" if name == "server" else "client.ext",
        )


class IdentityFlight(flight.FlightServerBase):
    def __init__(self, directory):
        super().__init__(
            "grpc+tls://localhost:0",
            tls_certificates=[
                (
                    (directory / "server.crt").read_bytes(),
                    (directory / "server.key").read_bytes(),
                )
            ],
            verify_client=True,
            root_certificates=(directory / "ca.crt").read_bytes(),
        )
        self.observations = []
        self.schemas = {
            name: pa.schema(
                [
                    pa.field(f"id_{name}", pa.int64(), nullable=True),
                    pa.field(f"key_{name}", pa.int64(), nullable=True),
                ]
            )
            for name in ["alice", "bob"]
        }

    def get_schema(self, context, descriptor):
        name = descriptor.path[-1].decode().removeprefix("remote_")
        return flight.SchemaResult(self.schemas[name])

    def do_get(self, context, ticket):
        identity = context.peer_identity().decode()
        assert identity in ["alice", "bob"], identity
        ctx = SessionContext()
        for name, schema in self.schemas.items():
            source = pa.Table.from_arrays(
                [
                    pa.array([1 if identity == "alice" else 2], type=pa.int64()),
                    pa.array([1], type=pa.int64()),
                ],
                schema=schema,
            )
            ctx.register_record_batches(f"remote_{name}", [source.to_batches()])
        plan = fields(fields(fields(ticket.ticket)[2])[1])[1]
        logical = Consumer.from_substrait_plan(ctx, Serde.deserialize_bytes(plan))
        frame = ctx.create_dataframe_from_logical_plan(logical)
        table = frame.to_arrow_table()
        self.observations.append(
            {
                "identity": identity,
                "plan_sha256": hashlib.sha256(plan).hexdigest(),
                "rows": table.to_pylist(),
            }
        )
        return flight.RecordBatchStream(table)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--source-commit",
        help="Commit that produced the prebuilt spiced binary; defaults to HEAD",
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
    pki(args.out)
    upstream = IdentityFlight(args.out)
    datasets = []
    for identity in ["alice", "bob"]:
        datasets.append(
            {
                "from": f"spice.ai:remote_{identity}",
                "name": identity,
                "params": {
                    "spiceai_endpoint": f"https://localhost:{upstream.port}",
                    "spiceai_tls_ca_certificate_file": str(args.out / "ca.crt"),
                    "spiceai_tls_client_certificate_file": str(
                        args.out / f"{identity}.crt"
                    ),
                    "spiceai_tls_client_key_file": str(args.out / f"{identity}.key"),
                },
            }
        )
    sidecar = Instance(
        args.spiced,
        args.out / "sidecar",
        base_pod("identity-scope", datasets, "sidecar"),
    )
    try:
        sidecar.start()
        sql = "SELECT a.id_alice AS aid, b.id_bob AS bid FROM alice a JOIN bob b ON a.key_alice = b.key_bob"
        plan, _ = sidecar.query("EXPLAIN " + sql)
        (args.out / "explain.json").write_text(json.dumps(plan, indent=2))
        actual, _ = sidecar.query(sql)
        observation = {"actual": actual, "expected": [{"aid": 1, "bid": 2}]}
        print(json.dumps(observation), flush=True)
        (args.out / "result.json").write_text(json.dumps(observation, indent=2))
        assert actual == observation["expected"], observation
        assert {item["identity"] for item in upstream.observations} == {
            "alice",
            "bob",
        }, upstream.observations
    finally:
        (args.out / "upstream-observations.json").write_text(
            json.dumps(upstream.observations, indent=2)
        )
        sidecar.stop()
        upstream.shutdown()


if __name__ == "__main__":
    main()
