#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Query live Hugging Face Hub datasets through spiced and diff every answer.

Each dataset is pinned to an immutable commit, so its expected answer cannot drift. The
answers were recorded with DuckDB's own `hf://` reader, which shares no code with Spice. When
`duckdb` is on PATH (or `--duckdb` names it) the script derives them again live, and both
Spice's and DuckDB's answers must equal the recorded ones.

Datasets cover Parquet (a single file, a glob, a folder whose format is inferred, and the
Hub's `refs/convert/parquet` branch through `@~parquet`), CSV through the Hub's
`resolve-cache`, newline-delimited JSON, and a TSV glob, each federated and accelerated.
Artifacts include every SQL result, the runtime log and the DuckDB answers.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

IMDB = "hf://datasets/stanfordnlp/imdb@e6281661ce1c48d982bc483cf8a173c1bbeb5d31"
GSM8K = "hf://datasets/openai/gsm8k@740312add88f781978c0658806c59bc2815b9866"
SCIFACT = "hf://datasets/mteb/scifact@cf10ab6856b15b0e670ef8ae5dae4e266c12d035"
IRIS = "hf://datasets/scikit-learn/iris@0bda0ce801be0fa2f464ff845a9d5ceae99aad7d"
# The commit of imdb's `refs/convert/parquet`, the branch `@~parquet` names: pinned, since the
# Hub may regenerate the conversion. The alias itself is covered by the parser's unit tests.
IMDB_CONVERTED = "hf://datasets/stanfordnlp/imdb@0b525c3ee2447b87002590030af0cdeaf509422a"

# name -> (from, Spice SQL, DuckDB SQL over the same files, recorded answer). The answers
# are integers so the engines cannot disagree on formatting.
CASES = {
    "imdb_test": (
        f"{IMDB}/plain_text/test-00000-of-00001.parquet",
        "SELECT count(*) AS n, sum(label) AS s, sum(character_length(text)) AS c FROM {t}",
        f"SELECT count(*) AS n, sum(label) AS s, sum(length(text)) AS c FROM '{IMDB}/plain_text/test-00000-of-00001.parquet'",
        {"n": 25000, "s": 12500, "c": 32344810},
    ),
    "imdb_folder": (
        f"{IMDB}/plain_text/",
        "SELECT count(*) AS n, sum(label) AS s, sum(character_length(text)) AS c FROM {t}",
        f"SELECT count(*) AS n, sum(label) AS s, sum(length(text)) AS c FROM '{IMDB}/plain_text/*.parquet'",
        {"n": 100000, "s": -25000, "c": 131966676},
    ),
    "imdb_glob": (
        f"{IMDB}/plain_text/t*-00000-of-00001.parquet",
        "SELECT count(*) AS n, sum(label) AS s, sum(character_length(text)) AS c FROM {t}",
        f"SELECT count(*) AS n, sum(label) AS s, sum(length(text)) AS c FROM '{IMDB}/plain_text/t*-00000-of-00001.parquet'",
        {"n": 50000, "s": 25000, "c": 65471551},
    ),
    "imdb_converted": (
        f"{IMDB_CONVERTED}/plain_text/test/",
        "SELECT count(*) AS n, sum(label) AS s, sum(character_length(text)) AS c FROM {t}",
        f"SELECT count(*) AS n, sum(label) AS s, sum(length(text)) AS c FROM '{IMDB_CONVERTED}/plain_text/test/*.parquet'",
        {"n": 25000, "s": 12500, "c": 32344810},
    ),
    "gsm8k_test": (
        f"{GSM8K}/main/test-00000-of-00001.parquet",
        "SELECT count(*) AS n, sum(character_length(question)) AS s, sum(character_length(answer)) AS c FROM {t}",
        f"SELECT count(*) AS n, sum(length(question)) AS s, sum(length(answer)) AS c FROM '{GSM8K}/main/test-00000-of-00001.parquet'",
        {"n": 1319, "s": 316390, "c": 386310},
    ),
    "scifact_queries": (
        f"{SCIFACT}/queries.jsonl",
        'SELECT count(*) AS n, sum(character_length("_id")) AS s, sum(character_length(text)) AS c FROM {t}',
        f"SELECT count(*) AS n, sum(length(_id)) AS s, sum(length(text)) AS c FROM read_json('{SCIFACT}/queries.jsonl', format='newline_delimited')",
        {"n": 1109, "s": 3553, "c": 98772},
    ),
    "scifact_qrels": (
        f"{SCIFACT}/qrels/*.tsv",
        'SELECT count(*) AS n, sum(score) AS s, sum(character_length(CAST("corpus-id" AS VARCHAR))) AS c FROM {t}',
        f"SELECT count(*) AS n, sum(score) AS s, sum(length(\"corpus-id\"::VARCHAR)) AS c FROM read_csv('{SCIFACT}/qrels/*.tsv', delim='\t', header=true)",
        {"n": 1258, "s": 1258, "c": 9514},
    ),
    "iris": (
        f"{IRIS}/Iris.csv",
        'SELECT count(*) AS n, sum("Id") AS s, CAST(round(sum("SepalLengthCm") * 10) AS BIGINT) AS c FROM {t}',
        f"SELECT count(*) AS n, sum(Id) AS s, CAST(round(sum(SepalLengthCm) * 10) AS BIGINT) AS c FROM read_csv('{IRIS}/Iris.csv', header=true)",
        {"n": 150, "s": 11325, "c": 8765},
    ),
}

# Full-row diffs for the tables small enough to compare every row: (Spice SQL, DuckDB SQL,
# recorded SHA-256 of DuckDB's rows as `row_hash` computes it).
ROW_DIFFS = {
    "iris": (
        'SELECT "Id", "SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm", "Species" FROM {t} ORDER BY "Id"',
        f"SELECT Id, SepalLengthCm, SepalWidthCm, PetalLengthCm, PetalWidthCm, Species FROM read_csv('{IRIS}/Iris.csv', header=true) ORDER BY Id",
        "ba4eaf75aeda79bb70cd9aa9b19300b4111e7a937ffce40c2d6ef7feaebd7a75",
    ),
    "gsm8k_test": (
        "SELECT question, answer FROM {t} ORDER BY question, answer",
        f"SELECT question, answer FROM '{GSM8K}/main/test-00000-of-00001.parquet' ORDER BY question, answer",
        "f356dccf3734ae1c1c94822393eddb8feced836dbe24964290ce4e01a03b2d52",
    ),
}


def request(url: str, query: str | None = None, timeout: float = 120):
    req = Request(
        url,
        data=query.encode() if query is not None else None,
        headers={"Content-Type": "text/plain", "Accept": "application/json"},
    )
    with urlopen(req, timeout=timeout if query is not None else 2) as response:
        body = response.read().decode()
        return json.loads(body) if query is not None else body


def ports() -> tuple[int, int]:
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(2)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return tuple(listener.getsockname()[1] for listener in sockets)


def duckdb_rows(duckdb: str, sql: str) -> list[dict]:
    output = subprocess.run(
        [duckdb, "-json", "-c", sql], capture_output=True, text=True, check=True, timeout=600
    ).stdout
    return json.loads(output) if output.strip() else []


def normalize(rows: list[dict]) -> list[dict]:
    """Integers and floats compare by value: DuckDB prints HUGEINT sums as strings."""
    normalized = []
    for row in rows:
        out = {}
        for key, value in row.items():
            if isinstance(value, str):
                with contextlib.suppress(ValueError):
                    value = int(value)
            out[key.lower()] = value
        normalized.append(out)
    return normalized


def row_hash(rows: list[dict]) -> str:
    canonical = json.dumps(normalize(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def run(spiced: Path, directory: Path, timeout: float, duckdb: str | None) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    http_port, flight_port = ports()
    endpoint = f"http://127.0.0.1:{http_port}"
    datasets = []
    for name, (location, *_rest) in CASES.items():
        datasets.append({"from": location, "name": f"{name}_federated"})
        datasets.append(
            {
                "from": location,
                "name": f"{name}_accelerated",
                "acceleration": {"enabled": True},
            }
        )
    params = {"hf_token": "${ env:HF_TOKEN }"} if os.environ.get("HF_TOKEN") else {}
    for dataset in datasets:
        if params:
            dataset["params"] = params
    # JSON is also valid YAML, keeping this harness dependency-free.
    (directory / "spicepod.yaml").write_text(
        json.dumps(
            {
                "version": "v1",
                "kind": "Spicepod",
                "name": "huggingface-live",
                "datasets": datasets,
                "runtime": {"caching": {"sql_results": {"enabled": False}}},
            },
            indent=2,
        )
    )
    results = []
    process = None
    try:
        with (directory / "spice.log").open("w") as log:
            process = subprocess.Popen(
                [str(spiced), "--http", f"127.0.0.1:{http_port}", "--flight", f"127.0.0.1:{flight_port}"],
                cwd=directory,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = time.monotonic() + timeout
            last = "runtime has not answered"
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise AssertionError(f"spiced exited with status {process.returncode}")
                try:
                    last = request(f"{endpoint}/v1/ready")
                    if last == "ready":
                        break
                except HTTPError as error:
                    last = error.read().decode()
                except (URLError, TimeoutError, ConnectionError) as error:
                    last = str(error)
                time.sleep(0.25)  # Poll the actual readiness condition until its deadline.
            else:
                raise AssertionError(f"Runtime did not become ready within {timeout}s: {last}")

            def sql(query: str) -> list[dict]:
                started = time.monotonic()
                rows = request(f"{endpoint}/v1/sql", query)
                elapsed = time.monotonic() - started
                results.append({"engine": "spice", "query": query, "seconds": elapsed, "rows": rows})
                print(f"spice {elapsed:6.2f}s {query}\n  {json.dumps(rows)[:300]}", flush=True)
                return rows

            def oracle(query: str) -> list[dict]:
                assert duckdb is not None
                started = time.monotonic()
                rows = duckdb_rows(duckdb, query)
                elapsed = time.monotonic() - started
                results.append({"engine": "duckdb", "query": query, "seconds": elapsed, "rows": rows})
                print(f"duckdb {elapsed:5.2f}s {query}\n  {json.dumps(rows)[:300]}", flush=True)
                return rows

            failures = []
            for name, (_, spice_sql, duckdb_sql, expected) in CASES.items():
                if duckdb:
                    live = normalize(oracle(duckdb_sql))
                    if live != [expected]:
                        failures.append(f"{name}: DuckDB answered {live}, recorded {expected}")
                for mode in ("federated", "accelerated"):
                    answer = normalize(sql(spice_sql.format(t=f"{name}_{mode}")))
                    if answer != [expected]:
                        failures.append(f"{name}_{mode}: Spice answered {answer}, expected {expected}")
            for name, (spice_sql, duckdb_sql, recorded) in ROW_DIFFS.items():
                want = None
                if duckdb:
                    want = normalize(oracle(duckdb_sql))
                    if row_hash(want) != recorded:
                        failures.append(f"{name}: DuckDB's rows hash to {row_hash(want)}, recorded {recorded}")
                for mode in ("federated", "accelerated"):
                    got = normalize(sql(spice_sql.format(t=f"{name}_{mode}")))
                    if row_hash(got) == recorded:
                        continue
                    detail = f"{len(got)} rows hash to {row_hash(got)}, recorded {recorded}"
                    if want is not None:
                        mismatch = next(
                            (i for i, (a, b) in enumerate(zip(got, want)) if a != b),
                            min(len(got), len(want)),
                        )
                        detail += f"; first difference from DuckDB's {len(want)} rows at row {mismatch}"
                    failures.append(f"{name}_{mode}: {detail}")
            assert not failures, "\n".join(failures)
            print(
                f"PASS: {len(datasets)} datasets ({len(CASES)} locations, federated and accelerated)"
                + (", diffed against DuckDB" if duckdb else ", against recorded DuckDB answers"),
                flush=True,
            )
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        (directory / "results.json").write_text(json.dumps(results, indent=2))
        print(f"Artifacts: {directory}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument(
        "--duckdb",
        default=shutil.which("duckdb"),
        help="DuckDB CLI to diff against (default: duckdb on PATH; omit to use the recorded answers)",
    )
    args = parser.parse_args()
    run(args.spiced.resolve(), args.artifacts.resolve(), args.timeout, args.duckdb)
