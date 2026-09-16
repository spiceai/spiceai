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

"""Origin server for the HTTP cache / rate-limit harness (Phase 0).

One FastAPI app. It serves a single data row whose integer ``version``
increases on a fixed cadence. A load generator reads that ``version``
back through Spice and compares it with the version the origin has
served most recently, so it can tell a FRESH response from a STALE one.

Endpoints:
  GET  /data     - the dataset source. Serves the current row.
  POST /control  - set ``bump_interval`` (and fault-mode stubs for Phase 1).
  GET  /stats    - aggregate counters plus the current high-water version.
  GET  /healthz  - liveness probe.

Every request to ``/data`` is appended to a JSONL request log before the
response is built, so the log is a true arrival record.

Phase 0 has no fault injection. ``error_rate``, ``mode``, ``error_status``,
``latency_ms`` and ``timeout_hang_ms`` are accepted by ``/control`` and
stored, but ``/data`` ignores them. Phase 1 wires them in.

Payload format is selectable so the harness can confirm empirically which
shape the Spice HTTP connector schema-infers cleanly:
  ORIGIN_PAYLOAD_FORMAT=ndjson  (default) - newline-delimited JSON objects.
  ORIGIN_PAYLOAD_FORMAT=array            - a single top-level JSON array.
  ORIGIN_PAYLOAD_FORMAT=csv              - a CSV document with a header row.
"""

import json
import os
import threading
import time
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

ORIGIN_NAME = os.environ.get("ORIGIN_NAME", "p1")
BUMP_INTERVAL_S = float(os.environ.get("ORIGIN_BUMP_INTERVAL", "1.0"))
PAYLOAD_FORMAT = os.environ.get("ORIGIN_PAYLOAD_FORMAT", "ndjson").lower()
REQUEST_LOG_PATH = os.environ.get("ORIGIN_REQUEST_LOG", "origin_requests.jsonl")
T0 = float(os.environ.get("HARNESS_T0", str(time.time())))
SEED_VERSION = int(os.environ.get("ORIGIN_SEED_VERSION", "1"))

_lock = threading.Lock()
_state = {
    "version": SEED_VERSION,
    "bump_interval": BUMP_INTERVAL_S,
    # Fault stubs (accepted, ignored in Phase 0).
    "error_rate": 0.0,
    "mode": "healthy",
    "error_status": 503,
    "latency_ms": {"base": 0, "jitter": 0},
    "timeout_hang_ms": 0,
    # Counters.
    "data_requests": 0,
    "total_requests": 0,
    "recent": [],  # recv_epoch_ms of recent /data hits, for a rolling rate
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _t_rel(recv_ms: int) -> float:
    return recv_ms / 1000.0 - T0


def _bump_loop() -> None:
    """Increase the version on a fixed cadence, independent of requests."""
    while True:
        interval = _state["bump_interval"]
        time.sleep(max(0.05, interval))
        with _lock:
            _state["version"] += 1


def _log_request(recv_ms: int, path: str, method: str, version_served: Optional[int]) -> None:
    row = {
        "recv_epoch_ms": recv_ms,
        "t_rel_s": round(_t_rel(recv_ms), 4),
        "origin": ORIGIN_NAME,
        "method": method,
        "path": path,
        "version_served": version_served,
    }
    # Append-only; one JSON object per line.
    with open(REQUEST_LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row) + "\n")


def _current_row(version: int, served_at_ms: int) -> dict[str, Any]:
    return {
        "id": 1,
        "version": version,
        "served_at_ms": served_at_ms,
        "origin": ORIGIN_NAME,
        "payload": f"v{version}",
    }


def _render_payload(row: dict[str, Any]) -> tuple[str, str]:
    """Return (body, media_type) for the configured payload format."""
    if PAYLOAD_FORMAT == "array":
        return json.dumps([row]), "application/json"
    if PAYLOAD_FORMAT == "csv":
        header = "id,version,served_at_ms,origin,payload"
        line = f'{row["id"]},{row["version"]},{row["served_at_ms"]},{row["origin"]},{row["payload"]}'
        return header + "\n" + line + "\n", "text/csv"
    # Default: newline-delimited JSON (one object per line).
    return json.dumps(row) + "\n", "application/x-ndjson"


app = FastAPI()


@app.on_event("startup")
def _startup() -> None:
    t = threading.Thread(target=_bump_loop, daemon=True)
    t.start()


def _serve_data(path: str) -> Any:
    recv_ms = _now_ms()
    with _lock:
        version = _state["version"]
        _state["data_requests"] += 1
        _state["total_requests"] += 1
        _state["recent"].append(recv_ms)
        # Keep only the last 10 seconds of arrivals.
        cutoff = recv_ms - 10_000
        _state["recent"] = [r for r in _state["recent"] if r >= cutoff]
    _log_request(recv_ms, path, "GET", version)
    body, media_type = _render_payload(_current_row(version, recv_ms))
    return PlainTextResponse(
        content=body,
        media_type=media_type,
        headers={"X-Origin-Version": str(version)},
    )


@app.get("/data")
def get_data() -> Any:
    return _serve_data("/data")


# File-like alias so the object-store listing connector fetches the URL with
# a plain GET instead of a WebDAV PROPFIND collection listing.
@app.get("/data.json")
def get_data_json() -> Any:
    return _serve_data("/data.json")


@app.head("/data.json")
def head_data_json() -> Any:
    # object_store may HEAD the object before a GET.
    recv_ms = _now_ms()
    with _lock:
        version = _state["version"]
    return PlainTextResponse(
        content="",
        media_type="application/x-ndjson",
        headers={"X-Origin-Version": str(version)},
    )


@app.post("/control")
async def post_control(request: Request) -> Any:
    profile = await request.json()
    recv_ms = _now_ms()
    with _lock:
        _state["total_requests"] += 1
        for key in (
            "bump_interval",
            "error_rate",
            "mode",
            "error_status",
            "latency_ms",
            "timeout_hang_ms",
        ):
            if key in profile:
                _state[key] = profile[key]
        applied = {
            "bump_interval": _state["bump_interval"],
            "error_rate": _state["error_rate"],
            "mode": _state["mode"],
            "error_status": _state["error_status"],
            "latency_ms": _state["latency_ms"],
            "timeout_hang_ms": _state["timeout_hang_ms"],
        }
    _log_request(recv_ms, "/control", "POST", None)
    return JSONResponse({"applied": applied, "server_epoch_ms": recv_ms})


@app.get("/stats")
def get_stats() -> Any:
    recv_ms = _now_ms()
    with _lock:
        cutoff = recv_ms - 10_000
        recent = [r for r in _state["recent"] if r >= cutoff]
        stats = {
            "origin": ORIGIN_NAME,
            "hwm_version": _state["version"],
            "bump_interval": _state["bump_interval"],
            "data_requests": _state["data_requests"],
            "total_requests": _state["total_requests"],
            "data_requests_last_10s": len(recent),
            "server_epoch_ms": recv_ms,
            "t_rel_s": round(_t_rel(recv_ms), 4),
        }
    return JSONResponse(stats)


@app.get("/healthz")
def healthz() -> Any:
    return PlainTextResponse("ok")
