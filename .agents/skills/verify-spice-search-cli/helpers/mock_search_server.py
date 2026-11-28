#!/usr/bin/env python3
"""Serve POST /v1/search, record request bodies, and return a fixed JSON document."""

from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import ClassVar


OK_BODY = json.dumps(
    {
        "results": [
            {
                "matches": {"body": "dolphins are mammals"},
                "_score": 0.91,
                "dataset": "docs",
                "primary_key": {"id": 1},
            }
        ],
        "duration_ms": 12,
    }
).encode()


class SearchHandler(BaseHTTPRequestHandler):
    record_dir: ClassVar[Path]
    status: ClassVar[int] = 200

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        n = len(list(self.record_dir.glob("body-*.json")))
        (self.record_dir / f"body-{n:02d}.json").write_bytes(body)
        (self.record_dir / f"path-{n:02d}.txt").write_text(self.path, encoding="utf-8")
        payload = OK_BODY if self.status == 200 else b'{"error":"boom"}'
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    record_dir = Path(sys.argv[1])
    status = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    host = os.environ.get("SEARCH_MOCK_HOST", "127.0.0.1")
    port = int(os.environ.get("SEARCH_MOCK_PORT", "0"))
    SearchHandler.record_dir = record_dir
    SearchHandler.status = status
    server = ThreadingHTTPServer((host, port), SearchHandler)
    bound = server.server_address[1]
    (record_dir / "port").write_text(str(bound), encoding="utf-8")
    print(bound, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
