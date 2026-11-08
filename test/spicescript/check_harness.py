#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Exercise negative and positive answer checks through the real runner and spiced."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


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
    ]
    args.work_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="spicescript-harness-") as directory:
        for name, steps, error in fixtures:
            fixture = Path(directory) / f"{name}.yaml"
            fixture.write_text(json.dumps({"name": name,
                "spicepod": {"version": "v1", "kind": "Spicepod", "name": name},
                "steps": [{"start": {}}] + steps}))
            result = subprocess.run([str(args.runner.resolve()), "--record-durations", "--spiced",
                str(args.spiced.resolve()), "--work-dir", str(args.work_dir.resolve() / name), str(fixture)],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            (args.work_dir / f"{name}.log").write_text(result.stdout)
            if error is not None:
                if result.returncode != 1 or error not in result.stdout:
                    raise SystemExit(f"{name}: expected a value-mismatch rejection containing {error!r}; "
                                     f"exit={result.returncode}\n{result.stdout}")
                print(f"{name}: rejected the incorrect answer")
            elif result.returncode != 0:
                raise SystemExit(f"{name}: equivalent maps were rejected\n{result.stdout}")
            else:
                print(f"{name}: accepted equivalent maps")
    print(f"{len(fixtures)} real-engine harness checks passed")


if __name__ == "__main__":
    main()
