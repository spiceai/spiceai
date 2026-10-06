#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Check the live GitHub smoke test's rows, including downloaded file content."""

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen


def check(endpoint: str, artifacts: Path):
    artifacts.mkdir(parents=True, exist_ok=True)
    for table, key, count in [
        ("issues", "number", 5),
        ("pulls", "number", 5),
        ("commits", "sha", 5),
        ("stargazers", "login", 5),
        ("files", "path", 1),
    ]:
        query = f"SELECT * FROM spiceai.{table}"
        req = Request(
            f"{endpoint}/v1/sql",
            data=query.encode(),
            headers={"Content-Type": "text/plain", "Accept": "application/json"},
        )
        with urlopen(req, timeout=60) as response:
            rows = json.load(response)
        (artifacts / f"{table}.json").write_text(json.dumps(rows, indent=2))
        assert len(rows) == count, f"{table}: expected {count} rows, got {rows!r}"
        keys = [row[key] for row in rows]
        assert all(keys) and len(set(keys)) == count, (
            f"{table}: invalid or duplicate keys {keys!r}"
        )
        if table == "files":
            assert rows[0]["path"] == "README.md", rows
            assert rows[0]["content"], "README.md content was not downloaded"
        print(
            f"PASS {table}: {count} rows, unique populated {key}; artifact {artifacts / f'{table}.json'}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    check(args.endpoint, args.artifacts)
