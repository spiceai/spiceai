# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Minimal urllib-based JSON / SQL client for the harness.

Kept dependency-free (standard library only) so the load generators need no
third-party HTTP client. ``sql`` never raises: any transport or HTTP error is
returned as a ``(status, body)`` pair so the caller can record it as a sample
rather than aborting the run.

``sql`` opens a connection per call, which is fine for a generator pacing
itself at a few queries a second. ``SqlSession`` keeps one connection open and
is what a saturating generator must use -- see its docstring for what happens
otherwise.
"""

from __future__ import annotations

import http.client
import json
import socket
import urllib.error
import urllib.request
from typing import Any


def get_json(url: str, timeout: float = 5.0) -> dict[str, Any]:
    """GET ``url`` and decode a JSON object. Raises on transport/HTTP error."""
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def post_json(url: str, payload: dict[str, Any], timeout: float = 5.0) -> Any:
    """POST ``payload`` as JSON and decode the JSON response. Raises on error."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def sql(url: str, query: str, timeout: float = 5.0) -> tuple[int, Any]:
    """POST a SQL string to spiced's ``/v1/sql`` and return ``(status, body)``.

    Never raises: an HTTP error yields ``(code, text)`` and any other transport
    error yields ``(0, "<ErrorType>: <message>")`` so the caller records it as a
    sample.
    """
    req = urllib.request.Request(
        url, data=query.encode(), headers={"Content-Type": "text/plain"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")
    except Exception as e:  # noqa: BLE001 - surface any transport error as a sample
        return 0, f"{type(e).__name__}: {e}"


class SqlSession:
    """A keep-alive connection to one spiced ``/v1/sql`` endpoint.

    One connection per worker, not one per query. A connection per query leaves
    a TIME_WAIT behind for 2*MSL -- 30s on macOS, against an ephemeral range of
    16,384 ports -- and a worker driving a throttled origin loops as fast as
    spiced can refuse it. A saturating generator therefore exhausts the host's
    ephemeral ports within minutes, after which every worker blocks on
    ``connect`` and the run measures the host's socket table instead of the
    rate limiter. That failure is slow, silent and self-healing, which makes it
    far worse than a crash: the scenarios in the middle of a long catalog
    quietly record almost no traffic and report a rate of zero.

    Not thread-safe: each worker owns one session.
    """

    #: Errors that mean the connection is spent but the request may be retried.
    _STALE = (
        http.client.BadStatusLine,
        http.client.CannotSendRequest,
        http.client.RemoteDisconnected,
        http.client.ResponseNotReady,
        ConnectionError,
        socket.timeout,
    )

    def __init__(self, host: str, port: int, path: str = "/v1/sql", timeout: float = 30.0):
        self.host = host
        self.port = port
        self.path = path
        self.timeout = timeout
        self._connection: http.client.HTTPConnection | None = None

    def _connect(self) -> http.client.HTTPConnection:
        if self._connection is None:
            self._connection = http.client.HTTPConnection(
                self.host, self.port, timeout=self.timeout
            )
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def query(self, sql: str) -> tuple[int, Any]:
        """Run one query. Returns ``(status, body)`` and never raises.

        A stale connection is retried once on a fresh one; anything else is
        returned as ``(0, "<ErrorType>: <message>")`` so the caller records it
        as a sample.
        """
        for attempt in (1, 2):
            try:
                connection = self._connect()
                connection.request(
                    "POST", self.path, body=sql.encode(), headers={"Content-Type": "text/plain"}
                )
                response = connection.getresponse()
                payload = response.read()
                if response.will_close:
                    self.close()
                try:
                    return response.status, json.loads(payload.decode())
                except (ValueError, UnicodeDecodeError):
                    return response.status, payload.decode(errors="replace")
            except self._STALE as error:
                self.close()
                if attempt == 2:
                    return 0, f"{type(error).__name__}: {error}"
            except Exception as error:  # noqa: BLE001 - surface anything else as a sample
                self.close()
                return 0, f"{type(error).__name__}: {error}"
        return 0, "unreachable"
