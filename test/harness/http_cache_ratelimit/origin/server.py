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

"""Origin server for the HTTP cache / rate-limit harness.

One FastAPI app. It serves a single data row whose integer ``version``
increases on a fixed cadence. A load generator reads that ``version``
back through Spice and compares it with the version the origin has
served most recently, so it can tell a FRESH response from a STALE one.

Endpoints:
  GET  /data     - the dataset source. Serves the current row, shaped by
                   the active fault profile.
  POST /control  - set the fault profile (and ``bump_interval``). Applies
                   immediately, no restart.
  GET  /stats    - aggregate counters plus the current high-water version(s).
  GET  /healthz  - liveness probe.

Every request to ``/data`` is appended to a JSONL request log after the
fault has been *decided* but before any fault delay is applied, so the
log is a true arrival record with the outcome that was chosen for it.

Fault modes (Section 4.4 of the harness plan). A ``/control`` profile
picks one ``mode`` and a fraction ``error_rate`` of requests receive the
fault; the rest are served a healthy row. Every response (healthy or
faulted, except ``refuse``) also carries the ``latency_ms`` slowdown.

  status  - return ``error_status`` (503 / 429 / ...) for the faulted
            fraction, with any ``headers`` attached.
  refuse  - abort the TCP connection with a RST, so the client (Spice's
            HTTP connector) sees a connection error, not an HTTP status.
  hang    - sleep ``timeout_hang_ms`` before responding, to force a
            Spice-side ``client_timeout``. Uses ``asyncio.sleep`` so it
            does not block the other requests in flight.
  latency - a non-failing slowdown only; ``latency_ms`` on every response
            and no errors regardless of ``error_rate``.

The fault draw uses a seeded RNG (``seed`` in the profile) so a run
replays deterministically.

Multi-key traffic: each distinct raw query string on ``/data`` is an
independent upstream request / cache key, so each advances its own
``version`` counter on the same bump cadence (see ``versions`` below). The
empty-string key is the default (a request that carries no query string).

Payload format is selectable so the harness can confirm empirically which
shape the Spice HTTP connector schema-infers cleanly:
  ORIGIN_PAYLOAD_FORMAT=ndjson  (default) - newline-delimited JSON objects.
  ORIGIN_PAYLOAD_FORMAT=array            - a single top-level JSON array.
  ORIGIN_PAYLOAD_FORMAT=csv              - a CSV document with a header row.

Launch (programmatically, so ``refuse`` can reach the live connection
registry that ``python -m uvicorn`` hides):
  python origin/server.py           # reads ORIGIN_* env vars
"""

import argparse
import asyncio
import json
import os
import random
import socket
import struct
import threading
import time
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

ORIGIN_NAME = os.environ.get("ORIGIN_NAME", "p1")
BUMP_INTERVAL_S = float(os.environ.get("ORIGIN_BUMP_INTERVAL", "1.0"))
PAYLOAD_FORMAT = os.environ.get("ORIGIN_PAYLOAD_FORMAT", "ndjson").lower()
REQUEST_LOG_PATH = os.environ.get("ORIGIN_REQUEST_LOG", "origin_requests.jsonl")
T0 = float(os.environ.get("HARNESS_T0", str(time.time())))
SEED_VERSION = int(os.environ.get("ORIGIN_SEED_VERSION", "1"))

# Keys a `/control` profile may set. Only these are copied onto the state, so
# an unexpected field in the request body is ignored rather than stored.
_CONTROL_KEYS = (
    "id",
    "bump_interval",
    "error_rate",
    "mode",
    "error_status",
    "latency_ms",
    "timeout_hang_ms",
    "headers",
    "seed",
    "fault_paths",
)

_lock = threading.Lock()
_state: dict[str, Any] = {
    # Per query-string key high-water version. Each distinct raw query
    # string on /data is an independent cache key; the empty key ("") is
    # the default for a request with no query string.
    "versions": {"": SEED_VERSION},
    "bump_interval": BUMP_INTERVAL_S,
    # Fault profile (see module docstring).
    "id": "healthy",
    "error_rate": 0.0,
    "mode": "healthy",
    "error_status": 503,
    "latency_ms": {"base": 0, "jitter": 0},
    "timeout_hang_ms": 0,
    "headers": {},
    "seed": 0,
    # Paths this fault applies to. Empty = every path on this origin (the
    # prior, path-agnostic behavior) -- set this to scope a fault to one
    # dataset's URL while a sibling dataset on the SAME origin (different
    # path, same host:port) stays healthy, to test whether the client-side
    # rate controller's per-origin state (keyed on host:port, not path --
    # see rate_control_key in data-http-rate-control) couples the two.
    "fault_paths": [],
    # Counters.
    "data_requests": 0,
    "total_requests": 0,
    "faulted_requests": 0,
    "recent": [],  # recv_epoch_ms of recent /data hits, for a rolling rate
    "request_seq": 0,
}

# Seeded RNG for the fault draw. Reset whenever a profile carries a seed.
_rng = random.Random(0)

# Populated in ``main()`` with the live uvicorn ``ServerState.connections``
# set, so ``refuse`` can find and abort the current connection's transport.
SERVER_STATE: dict[str, Any] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _t_rel(recv_ms: int) -> float:
    return recv_ms / 1000.0 - T0


def _version_for_key_locked(key: str) -> int:
    """Return the current version for a query-string key, seeding a new key at
    ``SEED_VERSION`` on first use. Callers must already hold ``_lock``."""
    versions = _state["versions"]
    version = versions.get(key)
    if version is None:
        version = SEED_VERSION
        versions[key] = version
    return version


def _bump_loop() -> None:
    """Increase the version on a fixed cadence, independent of requests."""
    while True:
        interval = _state["bump_interval"]
        time.sleep(max(0.05, interval))
        with _lock:
            # Advance every live query-string key independently on the same
            # cadence, so each cache key tracks its own version.
            versions = _state["versions"]
            for key in versions:
                versions[key] += 1


def _log_request(
    recv_ms: int,
    path: str,
    query: str,
    method: str,
    version_served: Optional[int],
    applied_status: Any = None,
    applied_delay_ms: Any = None,
    fault_profile_id: Optional[str] = None,
    request_seq: Optional[int] = None,
) -> None:
    row = {
        "recv_epoch_ms": recv_ms,
        "t_rel_s": round(_t_rel(recv_ms), 4),
        "origin": ORIGIN_NAME,
        "method": method,
        "path": path,
        # Raw query string of the request; the per-key cache/version key.
        "query": query,
        "applied_status": applied_status,
        "applied_delay_ms": applied_delay_ms,
        "fault_profile_id": fault_profile_id,
        "version_served": version_served,
        "request_seq": request_seq,
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


def _profile_headers(version: int) -> dict[str, str]:
    """Copy the profile's extra headers as plain strings."""
    hdrs = _state.get("headers") or {}
    out = {"X-Origin-Version": str(version)}
    for k, v in hdrs.items():
        out[str(k)] = str(v)
    return out


def _abort_connection(request: Request) -> bool:
    """Abort the current client's TCP connection with a RST.

    Finds the uvicorn protocol whose transport peer matches this request's
    client address in the live connection registry and aborts it, first
    forcing SO_LINGER=0 so the close is a RST the client reports as a
    connection error rather than a clean EOF. Returns True if a connection
    was aborted.
    """
    client = request.client
    conns = SERVER_STATE.get("connections")
    if not client or not conns:
        return False
    target = (client.host, client.port)
    for proto in list(conns):
        transport = getattr(proto, "transport", None)
        if transport is None:
            continue
        peer = transport.get_extra_info("peername")
        if peer and tuple(peer[:2]) == target:
            sock = transport.get_extra_info("socket")
            if sock is not None:
                try:
                    sock.setsockopt(
                        socket.SOL_SOCKET,
                        socket.SO_LINGER,
                        struct.pack("ii", 1, 0),
                    )
                except OSError:
                    pass
            transport.abort()
            return True
    return False


app = FastAPI()


@app.on_event("startup")
def _startup() -> None:
    t = threading.Thread(target=_bump_loop, daemon=True)
    t.start()


async def _serve_data(request: Request, path: str) -> Any:
    """Serve ``/data`` shaped by the active fault profile.

    Decides the outcome (fault draw + delay) first, records the arrival in
    the request log, and only then applies the delay / builds the
    response, so the logged ``recv_epoch_ms`` is a true arrival time.
    """
    recv_ms = _now_ms()
    query = request.url.query
    with _lock:
        version = _version_for_key_locked(query)
        _state["data_requests"] += 1
        _state["total_requests"] += 1
        _state["request_seq"] += 1
        seq = _state["request_seq"]
        _state["recent"].append(recv_ms)
        cutoff = recv_ms - 10_000
        _state["recent"] = [r for r in _state["recent"] if r >= cutoff]

        mode = _state["mode"]
        error_rate = float(_state["error_rate"])
        error_status = int(_state["error_status"])
        lat = _state["latency_ms"] or {}
        base = float(lat.get("base", 0))
        jitter = float(lat.get("jitter", 0))
        hang_ms = float(_state["timeout_hang_ms"])
        profile_id = _state.get("id")

        fault_paths = _state.get("fault_paths") or []
        path_faultable = not fault_paths or path in fault_paths
        faulted = (
            path_faultable and mode in ("status", "refuse", "hang") and _rng.random() < error_rate
        )
        if faulted:
            _state["faulted_requests"] += 1
        latency_delay_ms = base + (_rng.random() * jitter if jitter > 0 else 0.0)

        if faulted and mode == "refuse":
            applied_status: Any = "refuse"
            applied_delay_ms: float = 0.0
        elif faulted and mode == "hang":
            applied_status = "hang"
            applied_delay_ms = hang_ms
        elif faulted and mode == "status":
            applied_status = error_status
            applied_delay_ms = latency_delay_ms
        else:
            applied_status = 200
            applied_delay_ms = latency_delay_ms

    _log_request(
        recv_ms,
        path,
        query,
        "GET",
        version,
        applied_status=applied_status,
        applied_delay_ms=round(applied_delay_ms, 2),
        fault_profile_id=profile_id,
        request_seq=seq,
    )

    if applied_status == "refuse":
        aborted = _abort_connection(request)
        if aborted:
            # The connection is gone; the returned response is discarded.
            return Response(status_code=444)
        # Fallback if we could not reach the transport: a 503 still
        # records a Failure to the controller and a hard error to caching.
        await asyncio.sleep(applied_delay_ms / 1000.0)
        return PlainTextResponse(
            content="connection refused (fallback 503)",
            status_code=503,
            headers=_profile_headers(version),
        )

    if applied_status == "hang":
        await asyncio.sleep(applied_delay_ms / 1000.0)
        # The client (Spice) has almost certainly timed out and dropped the
        # socket by now; respond anyway (harmless if the peer is gone).
        with _lock:
            latest_version = _version_for_key_locked(query)
        body, media_type = _render_payload(_current_row(latest_version, _now_ms()))
        return PlainTextResponse(
            content=body, media_type=media_type, headers=_profile_headers(latest_version)
        )

    if applied_delay_ms > 0:
        await asyncio.sleep(applied_delay_ms / 1000.0)

    if applied_status != 200:
        # A faulted HTTP status. Carry the profile headers (Retry-After,
        # RateLimit, ...) so the cooldown / IETF paths can be exercised.
        return PlainTextResponse(
            content=f"origin fault: status {applied_status}",
            status_code=int(applied_status),
            headers=_profile_headers(version),
        )

    body, media_type = _render_payload(_current_row(version, recv_ms))
    return PlainTextResponse(
        content=body, media_type=media_type, headers=_profile_headers(version)
    )


@app.get("/data")
async def get_data(request: Request) -> Any:
    return await _serve_data(request, "/data")


# File-like alias so the object-store listing connector fetches the URL with
# a plain GET instead of a WebDAV PROPFIND collection listing.
@app.get("/data.json")
async def get_data_json(request: Request) -> Any:
    return await _serve_data(request, "/data.json")


@app.head("/data.json")
def head_data_json() -> Any:
    # object_store may HEAD the object before a GET.
    with _lock:
        version = _version_for_key_locked("")
    return PlainTextResponse(
        content="",
        media_type="application/x-ndjson",
        headers={"X-Origin-Version": str(version)},
    )


@app.post("/control")
async def post_control(request: Request) -> Any:
    profile = await request.json()
    recv_ms = _now_ms()
    global _rng
    with _lock:
        _state["total_requests"] += 1
        for key in _CONTROL_KEYS:
            if key in profile:
                _state[key] = profile[key]
        if "seed" in profile:
            _rng = random.Random(int(profile["seed"]))
        applied = {key: _state[key] for key in _CONTROL_KEYS}
    _log_request(recv_ms, "/control", "", "POST", None, fault_profile_id=applied["id"])
    return JSONResponse({"applied": applied, "server_epoch_ms": recv_ms})


@app.get("/stats")
def get_stats() -> Any:
    recv_ms = _now_ms()
    with _lock:
        cutoff = recv_ms - 10_000
        recent = [r for r in _state["recent"] if r >= cutoff]
        versions = dict(_state["versions"])
        stats = {
            "origin": ORIGIN_NAME,
            # Backward-compatible global high-water: the default (empty) key,
            # for callers that query without a request_query filter.
            "hwm_version": versions.get("", SEED_VERSION),
            # Per query-string key high-water map. An oracle compares a
            # query's observed version against the mark for the key it
            # actually used, not this global, so keys that diverge do not
            # raise false alarms.
            "hwm_by_key": versions,
            "bump_interval": _state["bump_interval"],
            "mode": _state["mode"],
            "fault_profile_id": _state["id"],
            "data_requests": _state["data_requests"],
            "total_requests": _state["total_requests"],
            "faulted_requests": _state["faulted_requests"],
            "data_requests_last_10s": len(recent),
            "server_epoch_ms": recv_ms,
            "t_rel_s": round(_t_rel(recv_ms), 4),
        }
    return JSONResponse(stats)


@app.get("/healthz")
def healthz() -> Any:
    return PlainTextResponse("ok")


def main() -> None:
    p = argparse.ArgumentParser(description="Harness origin server")
    p.add_argument("--host", default=os.environ.get("ORIGIN_HOST", "127.0.0.1"))
    p.add_argument(
        "--port", type=int, default=int(os.environ.get("ORIGIN_PORT", "9001"))
    )
    p.add_argument(
        "--log-level", default=os.environ.get("ORIGIN_LOG_LEVEL", "warning")
    )
    args = p.parse_args()

    config = uvicorn.Config(
        app, host=args.host, port=args.port, log_level=args.log_level
    )
    server = uvicorn.Server(config)
    # Expose the live connection registry so ``refuse`` can abort a
    # connection by RST from inside a request handler.
    SERVER_STATE["connections"] = server.server_state.connections
    server.run()


if __name__ == "__main__":
    main()
