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
"""

from __future__ import annotations

import json
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
