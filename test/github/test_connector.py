#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# SPDX-License-Identifier: Apache-2.0
"""Exercise the GitHub connector through spiced and a local GraphQL API.

No credentials or network services are required. The API enforces request
shape and pagination; SQL assertions exercise registration, federation and
acceleration. Artifacts include every API request, SQL result and runtime log.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROW_COUNT = 125
KINDS = ("issues", "pulls", "commits", "stargazers", "reviews")
TOKEN = "github-connector-fixture"
DATE = "2024-01-01T00:00:00Z"


def node(kind: str, number: int) -> dict:
    if kind == "reviews":
        # The validation probe's first parent has no children. A later scan
        # must still find every review beyond it with the declared schema.
        reviews = (
            []
            if number == 1
            else [
                {
                    "id": f"review-{number}",
                    "state": "APPROVED",
                    "author": {"login": "fixture-user"},
                    "commit": {"oid": f"{number:040x}"},
                }
            ]
        )
        return {
            "pull_request_id": f"PR-{number}",
            "pull_request_number": number,
            "reviews": {"totalCount": len(reviews), "nodes": reviews},
        }
    if kind == "stargazers":
        return {"starred_at": DATE, "node": {"login": f"user-{number}"}}
    if kind == "commits":
        return {"sha": f"{number:040x}", "message_head_line": f"commit-{number}"}
    return {
        "id": f"{kind}-{number}",
        "number": number,
        "title": f"{kind}-{number}",
        "state": "OPEN" if number % 2 else "CLOSED",
        "created_at": DATE,
        "updated_at": DATE,
        "closed_at": None if number % 2 else DATE,
        "author": {"author": "fixture-user"},
        "labels": {"labels": []},
        "assignees": {"assignees": []},
        "commits": {
            "commits_count": 26,
            "hashes": [{"id": f"{commit:040x}"} for commit in range(1, 26)],
        },
        "closing_issues_wrapper": {
            "closing_issues_count": 0,
            "closing_issues_references": [],
        },
    }


class GitHubAPI(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.requests: list[dict] = []
        self.errors: list[str] = []
        self.validated: set[str] = set()
        self.lock = threading.Lock()

    def graphql(self, query: str) -> dict:
        with self.lock:
            self.requests.append({"query": query})
            if "githubHealthCheck" in query:
                return {
                    "data": {
                        "githubHealthCheck": {
                            "id": "repository-fixture",
                            "nameWithOwner": "fixture/repository",
                        }
                    }
                }
            match = re.search(
                r"(pullRequests|issues|stargazers|history)\s*\([^)]*?first:\s*(\d+)",
                query,
            )
            if not match:
                self.errors.append(f"Unexpected query: {query}")
                return {"errors": [{"message": self.errors[-1]}]}
            connection, size = match[1], int(match[2])
            repository = re.search(r'name:\s*"([^"]+)"', query)[1]
            kind = repository.rsplit("_", 1)[0]
            cursor = re.search(r'after:\s*"(\d+)"', query)
            offset = int(cursor[1]) if cursor else 0
            self.requests[-1].update(
                kind=kind, repository=repository, size=size, offset=offset
            )
            if repository not in self.validated:
                # Reproduce GitHub's resource rejection at registration. A SQL
                # LIMIT applied only after registration cannot rescue this probe.
                if size != 1:
                    return {
                        "errors": [
                            {"message": "Resource limits for this query exceeded."}
                        ]
                    }
                self.validated.add(repository)
            if kind == "reviews" and not re.search(
                r"reviews\s*\(first:\s*100\)", query
            ):
                self.errors.append(
                    "The outer validation limit changed the nested reviews page size"
                )
                return {"errors": [{"message": self.errors[-1]}]}
            end = min(offset + size, ROW_COUNT)
            rows = [node(kind, n) for n in range(offset + 1, end + 1)]
            page = {
                "pageInfo": {"hasNextPage": end < ROW_COUNT, "endCursor": str(end)},
                "edges" if kind == "stargazers" else "nodes": rows,
            }
            if kind == "commits":
                return {
                    "data": {
                        "repository": {
                            "default_ref": {"ref": "trunk"},
                            "selected_ref": {
                                "ref": "trunk",
                                "target": {"history": page},
                            },
                        }
                    }
                }
            return {"data": {"repository": {connection: page}}}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        if self.path != "/graphql" or not self.headers.get(
            "Authorization", ""
        ).startswith(f"Bearer {TOKEN}-"):
            self.server.errors.append(f"Unexpected request: POST {self.path}")
            self.send_error(400)
            return
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        response = json.dumps(self.server.graphql(body["query"])).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


def request(url: str, query: str | None = None):
    req = Request(
        url,
        data=query.encode() if query is not None else None,
        headers={"Content-Type": "text/plain", "Accept": "application/json"},
    )
    with urlopen(req, timeout=60 if query is not None else 2) as response:
        body = response.read().decode()
        return json.loads(body) if query is not None else body


def ports() -> tuple[int, int]:
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(2)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return tuple(listener.getsockname()[1] for listener in sockets)


def run(spiced: Path, directory: Path, timeout: float):
    directory.mkdir(parents=True)
    server = GitHubAPI()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    http_port, flight_port = ports()
    endpoint = f"http://127.0.0.1:{http_port}"
    api_endpoint = f"http://127.0.0.1:{server.server_port}"
    datasets = []
    for kind in KINDS:
        for accelerated in (False, True):
            name = f"{kind}_{'accelerated' if accelerated else 'federated'}"
            dataset = {
                "from": f"github:github.com/fixture/{name}/{kind}",
                "name": name,
                "params": {
                    "github_endpoint": api_endpoint,
                    "github_token": f"{TOKEN}-{name}",
                },
            }
            if accelerated:
                dataset["acceleration"] = {
                    "enabled": True,
                    "refresh_sql": f"SELECT * FROM {name} LIMIT {ROW_COUNT}",
                }
            datasets.append(dataset)
    # JSON is also valid YAML, keeping this harness dependency-free.
    (directory / "spicepod.yaml").write_text(
        json.dumps(
            {
                "version": "v1",
                "kind": "Spicepod",
                "name": "github-fixture",
                "datasets": datasets,
                "runtime": {"caching": {"sql_results": {"enabled": False}}},
            },
            indent=2,
        )
    )
    process = None
    results = []
    try:
        with (directory / "spice.log").open("w") as log:
            process = subprocess.Popen(
                [
                    str(spiced),
                    "--http",
                    f"127.0.0.1:{http_port}",
                    "--flight",
                    f"127.0.0.1:{flight_port}",
                ],
                cwd=directory,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = time.monotonic() + timeout
            last = "runtime has not answered"
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise AssertionError(
                        f"spiced exited with status {process.returncode}"
                    )
                try:
                    last = request(f"{endpoint}/v1/ready")
                    if last == "ready":
                        break
                except HTTPError as error:
                    last = error.read().decode()
                except (URLError, TimeoutError) as error:
                    last = str(error)
                time.sleep(
                    0.1
                )  # Poll the actual readiness condition until its deadline.
            else:
                raise AssertionError(
                    f"Runtime did not become ready within {timeout}s: {last}"
                )

            def sql(query: str):
                rows = request(f"{endpoint}/v1/sql", query)
                results.append({"query": query, "rows": rows})
                print(f"SQL {query}\n{json.dumps(rows)}", flush=True)
                return rows

            for kind in KINDS:
                key = {"commits": "sha", "stargazers": "login", "reviews": "id"}.get(
                    kind, "number"
                )
                expected = [
                    n
                    if key == "number"
                    else f"{n:040x}"
                    if key == "sha"
                    else f"review-{n}"
                    if key == "id"
                    else f"user-{n}"
                    for n in range(2 if kind == "reviews" else 1, ROW_COUNT + 1)
                ]
                for mode in ("federated", "accelerated"):
                    table = f"{kind}_{mode}"
                    rows = sql(f"SELECT {key}, owner, repo FROM {table}")
                    assert sorted(row[key] for row in rows) == sorted(expected), (
                        table,
                        rows,
                    )
                    assert all(
                        row["owner"] == "fixture" and row["repo"] == table
                        for row in rows
                    )
                    requests_before = len(server.requests)
                    assert sql(f"SELECT {key} FROM {table} LIMIT 0") == []
                    assert len(server.requests) == requests_before, (
                        "LIMIT 0 made an API request"
                    )
                    assert len(sql(f"SELECT {key} FROM {table} LIMIT 2")) == 2
                    if mode == "federated" and kind != "reviews":
                        assert server.requests[-1]["size"] == 2, (
                            "LIMIT 2 was not pushed down"
                        )
                if kind in ("issues", "pulls"):
                    rows = sql(
                        f"SELECT number FROM {kind}_federated WHERE closed_at IS NULL ORDER BY number"
                    )
                    assert [row["number"] for row in rows] == list(
                        range(1, ROW_COUNT + 1, 2)
                    )
                if kind == "pulls":
                    rows = sql(
                        "SELECT commits_count, array_length(hashes) AS hashes_count "
                        "FROM pulls_federated LIMIT 2"
                    )
                    assert rows == [{"commits_count": 26, "hashes_count": 25}] * 2, rows
            assert not server.errors, server.errors
            assert server.validated == {dataset["name"] for dataset in datasets}, (
                server.validated
            )
            for repository in server.validated:
                requests = [
                    r for r in server.requests if r.get("repository") == repository
                ]
                assert requests[0]["size"] == 1, requests[0]
                assert any(r["offset"] > 0 for r in requests), (
                    f"{repository}: pagination not exercised"
                )
            print(
                "PASS: 10 datasets; exact keys; pagination, empty nested rows, NULLs and LIMITs",
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
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        (directory / "requests.json").write_text(json.dumps(server.requests, indent=2))
        (directory / "results.json").write_text(json.dumps(results, indent=2))
        print(f"Artifacts: {directory}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    for iteration in range(args.repeat):
        run(
            args.spiced.resolve(),
            args.artifacts.resolve() / str(iteration + 1),
            args.timeout,
        )
