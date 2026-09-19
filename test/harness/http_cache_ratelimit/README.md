# HTTP cache / rate-limit harness

A closed test rig that measures how a Spice.ai `refresh_mode: caching`
accelerator behaves in front of a live, changing HTTP origin. It decides —
from evidence (origin request logs, per-response samples, a shared clock) —
whether the cache serves fresh data, bounds staleness, shields the origin
from load, and (Phase 1) keeps serving stale data while the origin fails.

- **Phase 0** — one origin, one caching dataset, steady state. Proves the
  freshness oracle: fresh serve, bounded staleness, load absorption.
- **Phase 1** — origin fault injection + a shared-clock scenario driver.
  Proves (or refutes) RFC 5861 **stale-if-error (SIE)**: a failing origin
  should not stop the cache from serving the last good rows.

```
  loadgen (driver+oracle)          spiced (:8090)              origin (:9001)
  -----------------------          ----------------            --------------
  shared clock T0                  dataset d1                  GET  /data
  step origin fault profile  --->  duckdb, refresh_mode:  -->  POST /control
  poll SELECT ... WHERE origin      caching (max_age 3s,        (fault profile)
    compare version_seen vs hwm     swr 6s, sie enabled)       version bumps +1/s
```

## Components

- `origin/server.py` — FastAPI origin. Serves one row whose integer
  `version` increases each second. `POST /control` sets the fault profile
  (below); `GET /stats` reports the high-water `version` and counters;
  every `/data` GET is appended to `ORIGIN_REQUEST_LOG` (arrival time, the
  applied outcome, the fault id). Launch it with `python origin/server.py`
  (not `uvicorn origin.server:app`) so the `refuse` mode can reach the live
  connection registry it needs to reset a socket.
- `spicepod/spicepod.caching.yaml` — Phase 0 dataset (SWR only).
- `spicepod/spicepod.sie.yaml` — Phase 1 dataset, `caching_stale_if_error: enabled`.
- `spicepod/spicepod.sie.expiry.yaml` — duration-bounded SIE (PENDING, see below).
- `loadgen/run_phase0.py` — Phase 0 load generator + freshness oracle.
- `loadgen/run_phase1.py` — Phase 1 scenario driver + load generator + SIE
  oracle in one process (so the fault timeline and the samples share T0).
- `run_phase0.sh`, `run_phase1.sh` — one-command orchestration per phase.

## Origin fault modes (`POST /control`)

A profile picks one `mode`; a fraction `error_rate` (0..1) of `/data`
requests get the fault, drawn from a seeded RNG; the rest are served a
healthy row. Every response (except `refuse`) also carries the
`latency_ms` slowdown. The arrival is logged **before** any fault delay,
so the request log stays a true arrival record.

| mode | field(s) | effect | Spice sees |
|---|---|---|---|
| `status` | `error_status` (503/429), `headers` | return that HTTP status | retryable HTTP failure |
| `hang` | `timeout_hang_ms` | `asyncio.sleep` then respond | request timeout (a send error) if `> client_timeout` |
| `refuse` | — | abort the TCP connection with a RST | connection error (a send error) |
| `latency` | `latency_ms {base,jitter}` | slow but healthy | success, just slow |

Example:

```bash
curl -X POST http://127.0.0.1:9001/control -H 'Content-Type: application/json' \
  -d '{"id":"p1-503","mode":"status","error_status":503,"error_rate":1.0,
       "headers":{"Retry-After":"2"},"seed":12345}'
```

## Run it

```bash
cd test/harness/http_cache_ratelimit
# venv once (repo-root .venv is picked up automatically):
python3 -m venv ../../../.venv && ../../../.venv/bin/pip install -r requirements.txt

# Phase 0 (steady-state freshness):
QPS=10 DURATION_S=20 ./run_phase0.sh

# Phase 1 (stale-if-error scenarios):
./run_phase1.sh caching-sie-timeout     # MANDATORY
./run_phase1.sh caching-sie-refuse
./run_phase1.sh caching-sie-503
./run_phase1.sh caching-sie-expiry
```

Artifacts land in `$RUN_DIR` (Phase 1 default
`/tmp/http_cache_phase1_run/<scenario>/`): `samples.csv` (one row per
query), `driver_events.csv` (fault steps on the shared clock),
`assertions.json` (the verdict), `origin_p1.jsonl`, `spiced.log`. The exit
code is the verdict: **0 = PASS, 1 = FAIL, 2 = BLOCKED / PENDING**.

## Phase 1 scenarios and what the oracle asserts

Each scenario warms the cache (healthy), injects a fault at `t = warmup_s`,
recovers at `t = warmup_s + fault_s`. The **SIE window** is
`[warmup_s + max_age + swr, fault_end]` — the span where the entry is past
max-age + stale-while-revalidate AND the origin is failing, so the SIE
decision is what serves the response.

| scenario | fault | expected on the SIE feature | prebuilt v2.3.1 |
|---|---|---|---|
| `caching-sie-timeout` | `hang` > `client_timeout` | serve stale (send error) | **PASS** |
| `caching-sie-refuse` | connection reset | serve stale (send error) | **PASS** |
| `caching-sie-503` | HTTP 503 | serve stale (RFC 5861) | **BLOCKED** — returns empty |
| `caching-sie-expiry` | long error window | stale then fail-closed at the bound | **PENDING** — duration SIE not in binary |

Assertions (per run): `cache_never_ahead_of_origin` (correctness — never
serve a version the origin has not produced, checked against the origin
high-water mark sampled *after* the query so a blocked fetch does not false
-positive), `warmup_served_data`, `sie_serves_stale_through_error`
(stale served ≥1, never empty/error in the SIE window),
`sie_stale_version_is_frozen_and_below_hwm`,
`connector_timed_out_not_full_hang` (timeout scenario), and
`recovery_resumes_freshness`.

## Findings (verified against prebuilt `spiced` v2.3.1, `f3ca9d17dc`)

These are behaviors *observed by running the harness*, not code reading.

1. **Stale-if-error serves stale on a SEND error, but returns EMPTY on a
   503.** Past max-age + SWR with the origin failing, a connection timeout
   (`is_timeout`) or reset (`is_connect`) makes the cache serve the last
   good rows (SIE works). A retryable HTTP **503** instead yields an empty
   result set (`HTTP 200`, `[]`) — not the cached copy and not an error.
   RFC 5861 SIE should serve stale on a 5xx too, and an empty result is a
   silent-wrong-result risk. This is why `caching-sie-503` is reported
   BLOCKED rather than failed: SIE is present for send errors, absent for
   status errors. (Reproduce: `caching-sie-503` returns empty in the SIE
   window; `caching-sie-timeout` / `caching-sie-refuse` return stale.)

2. **`caching_stale_if_error` accepts only `enabled` / `disabled`; a
   duration is rejected at load.** `caching_stale_if_error: "60s"` fails
   dataset registration with *"Invalid 'caching_stale_if_error' value:
   '60s'. Expected 'enabled' or 'disabled'."* The duration-bounded SIE
   window (fail-closed once staleness passes the bound) is a newer feature
   (#14126) not in this binary, so `caching-sie-expiry` is PENDING that
   build. `spicepod.sie.expiry.yaml` is the config to use once it lands.

3. **`client_timeout` is enforced only with a BARE integer of seconds.** A
   suffixed duration (`"2s"`, `"500ms"`) is silently ignored: a 4s origin
   hang under `client_timeout: "2s"` waits the full 4s and returns 200,
   while `client_timeout: "1"` aborts the fetch at ~1s. The Phase 1
   spicepods use bare `"1"`. (The Phase 0 `spicepod.caching.yaml` still
   uses `"2s"`, which is a no-op there because Phase 0 injects no
   timeouts.)

Evidence for finding 1 (SIE window, per-response transitions):

```
caching-sie-timeout   t+32.17s  STALE  seen=13  hwm=32  lat=11205ms   -> serves stale on timeout
caching-sie-refuse    t+26.98s  STALE  seen=13  hwm=27  lat=6086ms    -> serves stale on reset
caching-sie-503       t+20.99s  EMPTY  seen=None hwm=21  lat=6149ms    -> returns [] on 503
```

Evidence for finding 3 (`caching-sie-timeout`, origin hang-fetch arrivals):
hang fetches re-arrive ~1.0s apart (client_timeout) though each hang would
only respond at +3000ms — the connector abandoned each attempt at the
timeout, it did not wait the full hang.

## Not in this phase

The second origin (p2), the adaptive rate-control assertions, the metrics
scraper, and docker-compose are later phases (see
`docs/dev/http_cache_ratelimit_harness_plan.md`, Sections 4.7 and 11).
