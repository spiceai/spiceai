#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Exercise negative and positive answer checks through the real runner and spiced."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


# The key both the upstream and a `.env` above the sidecar hold.
DOTENV_KEY = "harness-dotenv-key"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    fixtures = [
        ("nonempty", [{"query": "SELECT 1 AS value", "protocols": ["flightsql", "http"],
                        "expect-empty": True, "expect": {}}],
         "expected an empty answer, got 1 rows"),
        ("error-text", [{"query": "SELECT no_such_function('rejection_marker') AS value",
                         "protocols": ["flightsql"], "expect-error": "rejection_marker"}],
         "rejected, but the error lacks `rejection_marker`"),
        ("struct-value", [
            {"query": "SELECT named_struct('n', CAST(1 AS BIGINT)) AS value", "id": "before", "expect": {}},
            {"query": "SELECT named_struct('n', CAST(2 AS BIGINT)) AS value", "expect": {"same-as": "before"}}],
         "answer differs from the expected one"),
        ("map-null", [
            {"query": "SELECT map(['a'], [CAST(NULL AS BOOLEAN)]) AS value", "id": "before", "expect": {}},
            {"query": "SELECT map(['a'], [TRUE]) AS value", "expect": {"same-as": "before"}}],
         "answer differs from the expected one"),
        ("map-order", [
            {"query": "SELECT map(['a','b'], ['one', CAST(NULL AS VARCHAR)]) AS value", "id": "before", "expect": {}},
            {"query": "SELECT map(['b','a'], [CAST(NULL AS VARCHAR), 'one']) AS value", "expect": {"same-as": "before"}}],
         None),
        # A load checks every answer of every client, not just that queries ran.
        ("load-wrong", [{"load": {"queries": [{"sql": "SELECT 1 AS value", "expect": {"rows": [["2"]]}}],
                                  "clients": 4, "count": 200}}],
         "load query 1 answered wrong"),
        ("load-right", [{"load": {"queries": [{"sql": "SELECT 1 AS value", "expect": {"rows": [["1"]]}}],
                                  "clients": 4, "count": 200}}],
         None),
    ]
    args.work_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="spicescript-harness-") as directory:
        fixtures = [(name, steps, error, {}) for name, steps, error in fixtures]
        # A sidecar whose dataset names no key presents none, even with one in a
        # `.env` above its directory (as in a checkout): the upstream refuses it.
        # With the key set through `sidecar.env`, the same sidecar reads it.
        numbers = Path(directory) / "numbers.csv"
        numbers.write_text("n\n1\n2\n3\n")
        upstream = {"runtime": {"auth": {"api-key": {"enabled": True, "keys": [DOTENV_KEY]}}},
                    "datasets": [{"from": f"file:{numbers}", "name": "numbers", "params": {"file_format": "csv"}}]}
        sidecar_pod = {"version": "v1", "kind": "Spicepod", "name": "harness-app", "datasets": [
            {"from": "spice.ai:numbers", "name": "numbers",
             "params": {"spiceai_endpoint": "http://127.0.0.1:${instance.flight_port}"}}]}
        count = [{"query": "SELECT count(*) AS n FROM numbers", "expect": {"rows": [["3"]]}}]
        for name, sidecar, error in [("sidecar-dotenv", {"spicepod": sidecar_pod}, "dataset.auth"),
                                     ("sidecar-env-key", {"spicepod": sidecar_pod,
                                                          "env": {"SPICEAI_API_KEY": DOTENV_KEY}}, None)]:
            (args.work_dir / name).mkdir(parents=True, exist_ok=True)
            (args.work_dir / name / ".env").write_text(f"SPICEAI_API_KEY={DOTENV_KEY}\n")
            fixtures.append((name, count, error, {"spicepod": upstream, "sidecar": sidecar,
                                                  "defaults": {"api-key": DOTENV_KEY}}))
        for name, steps, error, extra in fixtures:
            fixture = Path(directory) / f"{name}.yaml"
            spicepod = {"version": "v1", "kind": "Spicepod", "name": name, **extra.pop("spicepod", {})}
            fixture.write_text(json.dumps({"name": name, "spicepod": spicepod,
                "steps": [{"start": {}}] + steps, **extra}))
            result = subprocess.run([str(args.runner.resolve()), "--record-durations", "--spiced",
                str(args.spiced.resolve()), "--work-dir", str(args.work_dir.resolve() / name), str(fixture)],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            (args.work_dir / f"{name}.log").write_text(result.stdout)
            if error is not None:
                if result.returncode != 1 or error not in result.stdout:
                    raise SystemExit(f"{name}: expected a failure containing {error!r}; "
                                     f"exit={result.returncode}\n{result.stdout}")
                print(f"{name}: failed with {error!r}, as it should")
            elif result.returncode != 0:
                raise SystemExit(f"{name}: equivalent answers were rejected\n{result.stdout}")
            else:
                print(f"{name}: accepted equivalent answers")
    print(f"{len(fixtures)} real-engine harness checks passed")


if __name__ == "__main__":
    main()
