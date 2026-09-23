# Local Test Harness Plan — `caching_stale_if_error` (RFC 5861) and `adaptive_rate_control`

Status: PLAN ONLY. Nothing here builds or changes product code.
Repo: `spiceai/spiceai`. Investigated against trunk `ff430a4b66`, worktree `/private/tmp/spiceai-worktrees/14126-stale-if-error` (branch `14126-stale-if-error`), and worktree `/private/tmp/spiceai-worktrees/14136-adaptive-rate-control` (branch `14136-adaptive-rate-control`, changes still uncommitted in the working tree).

## 1. Purpose and scope

This harness proves two Spice.ai runtime features by observed behavior, not by code reading:

1. RFC 5861 cache semantics for the `refresh_mode: caching` accelerator: stale-while-revalidate (SWR) and stale-if-error (SIE). HTTP connector timeouts must propagate as errors that trigger SIE.
2. Client-side adaptive rate control on the per-origin HTTP rate limiter: Google SRE-style client-side throttling (the one strategy #14143 ships).

Every claim the harness makes must rest on a reproduction: an origin request log, a scraped metric, a query result, or a response header. A plausible-looking pass is not a pass.

## 2. The key investigation result — how Spice signals cache status

This finding decides the whole load-generator design, so read it first.

### 2.1 Two different caches — do not confuse them

Spice has two independent caches. Only one exposes a per-response HTTP status header, and it is NOT the feature under test.

- SQL results cache (`runtime.results_cache`). This DOES set per-response headers.
  - `Results-Cache-Status: HIT | MISS | STALE | BYPASS`
  - `X-Cache: Hit from spiceai | Miss from spiceai`
  - `Results-Cache-Scope`, and `Cache-Control: max-age=<n>, stale-while-revalidate=<n>`
  - Evidence: `crates/runtime/src/http/v1/mod.rs:585-649` (`attach_cache_headers`, header insert at line 596) and the enum `crates/cache/src/result/mod.rs:40-63` (`CacheStatus::to_header_string` maps to `"HIT"`, `"MISS"`, `"STALE"`, `"BYPASS"`).
- Acceleration `refresh_mode: caching` (the feature under test). This does NOT set any per-response HIT/STALE/MISS header, and it has NO dedicated hit/miss/stale metric counter.
  - The acceleration metric family is defined in `crates/runtime-metrics/src/acceleration.rs`. It has refresh-oriented metrics only: `dataset_acceleration_refresh_errors`, `dataset_acceleration_last_refresh_unix_time_ms`, `dataset_acceleration_refresh_duration_ms`, `dataset_acceleration_size_bytes`, and similar. There is no `cache_hit`/`cache_stale`/`cache_miss` counter.
  - The serving path returns row data. It marks each cached row with an internal fetch-time column `_fetched_at` (`CACHE_REFRESHED_AT_COLUMN = "_fetched_at"`, `crates/runtime-table/src/accelerated/caching.rs:125`). The freshness decision reads that column (`first_fetched_at_nanos`, `caching.rs:707`; `staleness_past_max_age`, used at `caching.rs:2023`).

### 2.2 Consequence for the harness (mandatory design rule)

For the caching feature the load generator CANNOT read a HIT/STALE/MISS header. It MUST infer freshness from the response payload. Use two payload signals:

1. Origin monotonic version. Each origin serves a monotonically increasing integer `version` (and a `served_at` epoch-ms) in every response body. This is the primary oracle. The load generator compares the `version` it reads back through Spice against the highest `version` the origin has ever served. Equal-or-newer = fresh; older = stale.
2. `_fetched_at` (secondary, treat as uncertain). A query MAY be able to `SELECT _fetched_at` from a caching dataset (the name is not on the reserved list — `caching.rs:2521` asserts `!is_reserved_caching_column("_fetched_at")`). Whether `SELECT *` surfaces it is unconfirmed. Do NOT depend on it. Confirm empirically in Phase 0; if it works, use it as a cross-check on the version oracle, not as the sole signal.

If a scenario also puts the SQL results cache in front (not the default plan), the harness may additionally assert on `Results-Cache-Status`. Keep the results cache OFF by default so the two caches never mask each other.

### 2.3 Adaptive rate-control observability (confirmed, metrics-based)

Rate control is fully observable through `runtime.metrics` (Prometheus/OpenTelemetry). The metrics are registered as OTel observable instruments in `crates/data-http-rate-control/src/lib.rs:363-472` and are labeled per origin by an `origin` attribute (`callback_to_observe_metric`, `lib.rs:522-532`; the label value is `rate_control_key(base_url)`).

Adaptive metrics (per origin) — only these two gauges are registered; #14143 exposes no separate throttled-request counter, so "was this request throttled" is read off the admission coefficient dropping below 1000:
- `adaptive_rate_control_effective_limit` (gauge; `0` when disabled)
- `adaptive_rate_control_admission_coefficient_permille` (gauge, `0..=1000`; `1000` = admit all)

Cooldown/Retry-After metrics (per origin):
- `rate_limit_retry_after_updates_total`, `rate_limit_retry_after_waits_total`, `rate_limit_retry_after_wait_duration_ms`, `rate_limit_retry_after_remaining_ms`

Static limiter metrics (per origin): `inflight_operations`, `rate_control_available_permits`, `rate_control_acquisitions_total`, `rate_control_acquire_errors_total`, `rate_control_wait_duration_ms`, `rate_control_requests_per_second_limit`, `rate_control_requests_per_minute_limit`, `rate_control_max_concurrent_requests`, `rate_control_jitter_min_ms`, `rate_control_jitter_max_ms`.

Note on IETF `RateLimit`/`RateLimit-Policy` advertised-quota headers: a code comment marks these as not yet honored (`lib.rs:444-446`, "TODO(#14136): honor server-advertised RateLimit/RateLimit-Policy headers"). The harness must still be able to emit those headers from the origins so a later expected-outcome flip is a config change, not a rebuild. Today assert only that they do NOT change admission; when the feature lands, flip the assertion.

### 2.4 Outcome classification (what counts as failure for the controller)

Confirmed in `crates/data_components/src/http/provider.rs:1608-1628`:
- A send error (timeout or connection error) records `RequestOutcome::Failure` (line 1613).
- A response with a retryable status (408 / 429 / 5xx) records `Failure`; any other status records `Success` (lines 1624-1628). `status_is_retryable` lives in `crates/data_components/src/resilient_http.rs:310-316`; connect/timeout detection uses `error.is_connect() || error.is_timeout()` (`resilient_http.rs:295`).

This is why the timeout fault mode is mandatory: a hang past `client_timeout` is the same failure signal to the controller as a 503, and it is the trigger for SIE on the caching side.

## 3. Architecture

```
                          shared wall clock (single monotonic source, e.g. NTP-free: harness start T0)
                                    |
      +----------------- Scenario Driver (Python) ------------------+
      |  reads timeline.yaml; at each step POSTs fault profiles     |
      |  to origin control APIs; stamps every event with t = now-T0 |
      +----+---------------------------+----------------------+-----+
           | control (HTTP POST)        | control              | control
           v                            v                      v
     +-------------+              +-------------+        (p2 shared by 2 datasets)
     | Origin p1   |              | Origin p2   |
     | :9001       |              | :9002       |
     | /data       |              | /data       |
     | /control    |              | /control    |
     | /stats      |              | /stats      |
     | req log     |              | req log     |
     +------+------+              +------+------+
            ^  (HTTP GET data fetch, subject to per-origin rate control)
            |                            ^          ^
            |                            |          |
      +-----+----------------------------+----------+------+
      |                 spiced (one instance)              |
      |  dataset d1  -> origin p1  (refresh_mode: caching) |
      |  dataset d2a -> origin p2  (caching)               |
      |  dataset d2b -> origin p2  (caching)               |
      |  rate control per origin: p1 limiter, p2 limiter   |
      |  HTTP :8090 (SQL/Flight)   metrics :9090/metrics   |
      +----+----------------------------------------+------+
           ^ SQL queries (target QPS)               | scrape /metrics every 1s
           |                                        v
     +-----+-----------+                     +------+-----------+
     | Load Generator  |  per-response CSV   | Metrics Scraper  |
     | (Python asyncio)|-------------------->| -> metrics.csv   |
     +-----------------+   record schema     +------------------+
           |                                        |
           +----------------> Run Directory <-------+
                    config + spiced commit + all logs +
                    metrics.csv + assertions.json + correlation.png
```

Data flow: Load generator -> spiced SQL -> caching accelerator -> (on miss/expired) HTTP connector -> origin. Observability flow: origins write request logs; spiced exposes `/metrics`; the scraper samples them; the oracle correlates all three on the shared clock.

## 4. Component-by-component specification

### 4.1 Origin servers p1, p2

One Python program, two instances on different ports (two ports = two origins = two independent rate-control keys; `rate_control_key` derives from the base URL host:port).

Endpoints:
- `GET /data` — the dataset source. Returns the current payload for this origin. Behavior is shaped by the active fault profile (Section 4.4). Records the request in the request log before applying any fault delay, so the log is a true arrival record.
- `POST /control` — runtime control API. Body is a JSON fault profile (Section 4.4). Applies immediately, no restart. Returns the applied profile and the server clock so the driver can measure control latency.
- `GET /stats` — returns aggregate counters (total requests, per-status counts, current in-flight, requests in the last N seconds) as JSON. Cheap health/inspection.
- `GET /healthz` — liveness for compose.

Payload and versioning (the oracle source):
- The origin holds a monotonic `version` integer, incremented on a fixed cadence (default: +1 every `bump_interval` seconds, default 1s) by a background task, independent of requests. This models an upstream whose data changes over time so the harness can tell fresh from stale.
- `GET /data` returns rows shaped for a Spice dataset, for example JSON:
  ```json
  {"rows": [
    {"id": 1, "version": 42, "served_at_ms": 1731000000123, "origin": "p1", "payload": "v42"}
  ]}
  ```
- The response echoes the version in a body field (primary oracle) AND in a response header `X-Origin-Version` (convenience for the load generator; independent of Spice cache headers).
- Deterministic mode: a seed fixes the version schedule and the pseudo-random fault draws, so a run replays exactly.

Request-log format (one line per received request, append-only CSV or JSONL):
```
recv_epoch_ms, t_rel_s, origin, method, path, applied_status, applied_delay_ms,
  fault_profile_id, version_served, client_ip, request_seq
```
`t_rel_s = recv_epoch_ms/1000 - T0`. `fault_profile_id` ties the row to the driver step that was active. This log is the ground truth for "did faults actually throttle upstream requests".

### 4.2 Origin control API and fault profiles (Section 4.4 defines modes)

`POST /control` accepts a JSON profile:
```json
{
  "id": "p2-503-latency",
  "error_rate": 0.5,
  "error_status": 503,
  "latency_ms": {"base": 20, "jitter": 10},
  "mode": "status",
  "timeout_hang_ms": 0,
  "headers": {"Retry-After": "2", "RateLimit": "limit=100, remaining=0, reset=2"},
  "seed": 12345
}
```
Fields:
- `error_rate` — fraction of requests that get the fault (0..1), drawn from the seeded RNG.
- `mode` — one of `status`, `refuse`, `hang` (see 4.4).
- `error_status` — status to return in `status` mode (503 or 429 etc.).
- `latency_ms` — base + jitter added to EVERY response (fresh or faulted); models a slow-but-alive origin.
- `timeout_hang_ms` — in `hang` mode, how long to sleep before responding (set larger than the dataset `client_timeout` to force a Spice-side timeout).
- `headers` — extra response headers to attach (`Retry-After`, `X-RateLimit-*`, `RateLimit`, `RateLimit-Policy`).

### 4.3 The spiced instance and spicepods

One spiced process. Topology: d1 -> p1; d2a and d2b -> p2. Two origins prove cross-origin isolation; two datasets on p2 prove same-origin limiter sharing.

Runtime section (rate control is a runtime.params concern; the keys are the `http_*` family in `HTTP_RATE_CONTROL_RUNTIME_PARAMS`, `crates/data-http-rate-control/src/lib.rs:64-71`):
```yaml
runtime:
  params:
    # http_adaptive_rate_control: disabled | enabled — one admission-coefficient
    # strategy (Google SRE client-side throttling), not a selectable AIMD/SRE
    # pair. K is set via http_adaptive_rate_control_failure_threshold
    # (K = 1/(1-threshold), default threshold 50% => K = 2.0), decay half-life
    # via http_adaptive_rate_control_window (default 10s).
    http_adaptive_rate_control: enabled
    http_requests_per_second_limit: "20"   # sets the admission ceiling for the origin
    http_max_concurrent_requests: "8"
    # http_requests_per_minute_limit, http_rate_control_jitter_min, _max are also valid
  telemetry:
    enabled: true    # exposes /metrics for the scraper
```
Note: the ceiling the controller grows back toward is the max of the configured rps/rpm/concurrency, else `DEFAULT_ADAPTIVE_CEILING = 100` (`lib.rs:160-177`, `crates/data_components/src/rate_limit/adaptive.rs:46`). Rate control is applied per origin, so a single runtime setting governs both p1 and p2 limiters independently; the harness reads per-origin effect from the `origin`-labeled metrics.

Caching datasets (the acceleration params live on the dataset, parsed in `crates/runtime-acceleration/src/acceleration.rs`):
```yaml
datasets:
  - from: https://p1.local:9001/data
    name: d1
    params:
      client_timeout: "2"       # seconds; forces SIE when origin hangs past this (https.rs:865)
      connect_timeout: "1"      # seconds (https.rs:873)
      file_format: json
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: caching
      params:
        caching_ttl: "3s"                       # max-age (aka caching_item_ttl)
        caching_stale_while_revalidate_ttl: "6s"  # SWR window beyond max-age
        caching_stale_if_error: "60s"           # SIE window; also: enabled | disabled
      # a retention policy is required (or SIE=enabled warns); keep entries long enough
      retention:
        enabled: true
        period: "10m"

  - from: https://p2.local:9002/data
    name: d2a
    params: { client_timeout: "2", connect_timeout: "1", file_format: json }
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: caching
      params: { caching_ttl: "3s", caching_stale_while_revalidate_ttl: "6s", caching_stale_if_error: "60s" }
      retention: { enabled: true, period: "10m" }

  - from: https://p2.local:9002/data
    name: d2b
    params: { client_timeout: "2", connect_timeout: "1", file_format: json }
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: caching
      params: { caching_ttl: "3s", caching_stale_while_revalidate_ttl: "6s", caching_stale_if_error: "60s" }
      retention: { enabled: true, period: "10m" }
```
`caching_stale_if_error` accepts `disabled`, `enabled` (unbounded), or a duration such as `60s` (parsed in `acceleration.rs:1245`, `parse_caching_stale_if_error`). `enabled` with no retention triggers a startup warning (`crates/runtime/src/datafusion/caching_retention.rs:168`), so pair it with retention.

Provide a small matrix of spicepods (one per top-level scenario group) rather than one giant file: `spicepod.caching.yaml` (rate control disabled, tiny windows), `spicepod.ratecontrol.sre.yaml` (`http_adaptive_rate_control: enabled`, `http_adaptive_rate_control_failure_threshold: "50%"` => K=2.0 — shared by all adaptive-rate-control scenarios, since #14143 ships one strategy), `spicepod.composition.yaml` (both features on), and `spicepod.slow.yaml` (real-duration windows). Duration scaling lives here (Section 8).

### 4.4 Origin fault modes (each separately dialable)

- `status` 503 — return HTTP 503 for the faulted fraction. Failure to the controller; a hard error to the caching path.
- `status` 429 — return HTTP 429, optionally with `Retry-After`/`RateLimit` headers. Failure to the controller; also drives the cooldown metrics.
- `refuse` (connection-refused) — close the socket without a response, or bind-drop so the connect fails. Exercises `error.is_connect()`.
- `hang` (timeout) — sleep `timeout_hang_ms` (> dataset `client_timeout`) then optionally respond. MANDATORY: this is the timeout path that both triggers SIE (caching) and records a Failure via `error.is_timeout()`.
- `latency` — a non-failing slowdown (base+jitter) layered onto any mode; proves latency alone does not trip the controller while a timeout does.
- Header toggles — attach `Retry-After`, `X-RateLimit-Limit/Remaining/Reset`, `RateLimit`, `RateLimit-Policy` on any response. Used both for the cooldown assertions and for the not-yet-honored IETF headers (Section 2.3).

Each mode is independently dialable per origin and can be combined (e.g. 50% 503 plus 15ms latency on p2 while p1 stays clean).

### 4.5 Load generator

Responsibilities:
- Drive spiced at a target QPS per dataset with an open-loop arrival schedule (Poisson or fixed-interval), so a slow/blocked SIE fetch does not silently lower offered load (closed-loop would hide overshoot). Use an asyncio task pool with a token/interval pacer.
- For each response, parse the returned rows, extract the `version` (primary oracle) and `served_at_ms`, and compare against the origin high-water version the driver publishes on the shared clock.
- Record every response.

Per-response record schema (CSV/Parquet):
```
send_epoch_ms, t_rel_s, dataset, target_qps, latency_ms, http_status,
  ok(bool), rows, version_seen, origin_hwm_version, freshness(FRESH|STALE|UNKNOWN),
  error_kind(none|timeout|http_5xx|http_429|conn_refused|parse),
  results_cache_status(optional header), scenario_step_id
```
`freshness` is derived: `FRESH` if `version_seen >= origin_hwm_version_at_send`; `STALE` if `version_seen < hwm`; `UNKNOWN` if no rows/parse failed. Because SWR serves stale-then-refreshes, expect a bounded lag, not exact equality; the oracle encodes the tolerance (Section 6).

QPS control approach: open-loop scheduler with a fixed inter-arrival (or exponential for Poisson), decoupled from response completion; concurrency cap high enough that pacing, not the pool, sets the rate; per-second achieved-QPS is computed from `send_epoch_ms` buckets for the report.

### 4.6 Scenario driver

- Reads `timeline.yaml`: an ordered list of steps `{ at_s, target: p1|p2, profile: {...}, note }`.
- At `T0` records the wall clock; every subsequent action is scheduled at `T0 + at_s`.
- At each step it POSTs the profile to the origin control API and appends a driver-event row: `t_rel_s, action, target, profile_id`.
- Publishes the per-origin version high-water mark on the shared clock so the load generator and oracle agree on "what fresh means at time t".
- Seeded: the same `timeline.yaml` + seed replays identically.

Timeline format example:
```yaml
seed: 12345
steps:
  - { at_s: 0,   target: p2, profile: healthy }
  - { at_s: 30,  target: p2, profile: { id: p2-503-lat, mode: status, error_status: 503, error_rate: 0.5, latency_ms: {base: 20, jitter: 10} } }
  - { at_s: 90,  target: p2, profile: healthy }
  - { at_s: 150, target: p2, profile: { id: p2-timeout, mode: hang, timeout_hang_ms: 5000 } }
  - { at_s: 210, target: p2, profile: healthy }
```

### 4.7 Metrics scraper

- Polls `http://spiced:9090/metrics` every 1s. Parses the Prometheus text format. Keeps only the harness-relevant series (Section 2.3 rate-control names, plus `dataset_acceleration_*`).
- Writes long-format `metrics.csv`: `scrape_epoch_ms, t_rel_s, metric_name, origin_or_dataset_label, value`.
- Records the reset behavior of counters (monotonic) so the oracle can diff over a window.

### 4.8 Assertion / oracle layer

Consumes: origin request logs, load-generator records, `metrics.csv`, driver events — all keyed on `t_rel_s`. Emits `assertions.json` (per-check pass/fail with the evidence rows that justify it) and a non-zero exit code on any failure. Produces one correlation plot (`correlation.png`): stacked timeline of offered vs achieved QPS, response freshness, error kinds, origin arrival rate, `admission_coefficient_permille`, and `effective_limit`, all on the shared clock. The plot is an aid; the pass/fail is the JSON.

## 5. RFC 5861 truth table (the caching oracle), grounded in the code

Freshness bands for entry age `a`, with `M = caching_ttl` (max-age), `W = caching_stale_while_revalidate_ttl`, `E = caching_stale_if_error` window. Decision logic confirmed in `crates/runtime-table/src/accelerated/caching.rs` (`handle_cache_hit` doc at 2054-2060; miss/error path 2010-2051):

| Entry age | Origin healthy | Origin erroring/timing out |
|---|---|---|
| `a <= M` (Fresh) | Serve cached, no fetch | Serve cached, no fetch |
| `M < a <= M+W` (Stale/SWR) | Serve cached immediately; background refresh | Serve cached immediately; background refresh attempt fails silently |
| `a > M+W` (Expired) | Treat as miss: block, fetch, serve fresh | If `within_error_window(staleness)` -> serve stale (SIE); else propagate origin error |

`within_error_window` (`caching.rs:2024`): `enabled` = always true; `<duration> E` = staleness within `E`; `disabled` = never (fail closed). Timeout reaches this path because the connector send error surfaces as an `Err` fetch (Section 2.4), and `client_timeout` sets when that fires.

## 6. Concrete pass/fail checks

Caching (per scenario window):
- SWR-serves-stale: while `M < a <= M+W` and origin healthy, every response is `ok=true` and p95 latency is near a cache read (well under origin latency), AND the origin request log shows background refreshes (roughly one per SWR interval per key), not one per query.
- SWR-refreshes: after a version bump, `version_seen` catches up to the new high-water within `<= M+W+bump_interval` (bounded lag), proving the background refresh landed.
- SIE-on-503: during a p2 hard-error window, once entries pass `M+W`, responses stay `ok=true` and `freshness=STALE` as long as staleness `<= E`; the origin log shows fetch attempts that returned 503.
- SIE-on-timeout (mandatory): with `mode: hang`, `timeout_hang_ms > client_timeout`, responses stay `ok=true` STALE within `E`; assert the connector actually timed out (origin log shows the request arrived and hung; load-gen latency for the underlying refresh attempt is near `client_timeout`, not near `timeout_hang_ms`).
- SIE-expiry: extend the error window past `E`; assert responses flip to `ok=false` (error propagated), proving the window is bounded and fail-closed.
- Fresh-band-no-fetch: while `a <= M`, the origin request-log arrival count is ~0 beyond the periodic refreshes, proving fresh entries are served without upstream calls.

Rate control (per origin, diffed over the fault window):
- Backoff-on-failure: during the p2 error window, `adaptive_rate_control_admission_coefficient_permille{origin=p2}` drops well below 1000; the p2 origin arrival rate (from its request log) falls below the offered rate. (No separate throttled-request counter exists — see 2.3.)
- SRE shape: with `http_adaptive_rate_control: enabled` and `http_adaptive_rate_control_failure_threshold: "50%"` (K=2.0), admission ≈ `min(1, (K*accepts+1)/(requests+1))` within tolerance, using accept/request counts reconstructed from the origin log over the 10s decaying window (`SRE_WINDOW_HALF_LIFE`, `adaptive.rs:52`).
- Recovery: after the origin heals, `admission_coefficient_permille` returns to 1000 and `effective_limit` climbs back to the ceiling within a bounded time.
- Cooldown headers: when the origin sends `Retry-After`/`RateLimit`, `rate_limit_retry_after_updates_total` and `rate_limit_retry_after_remaining_ms` move; the origin arrival log shows a gap of about the advertised duration.
- IETF advertised-quota headers (today): with only `RateLimit`/`RateLimit-Policy` set (no `Retry-After`, no 429 body), assert admission does NOT change — encodes the current not-yet-honored behavior (Section 2.3). Flip to the opposite assertion when #14136 lands that support.

Topology:
- Cross-origin isolation: a p2-only fault throttles both p2 datasets (d2a, d2b) but leaves p1 (d1) offered≈achieved QPS and `admission_coefficient_permille{origin=p1} == 1000`.
- Same-origin sharing: d2a and d2b share ONE p2 limiter. Assert one `origin=p2` metric series exists (not two), and the sum of d2a+d2b upstream arrivals is bounded by the single p2 limit, not double it.

## 7. Scenario catalog

1. `caching-swr-basic` (spicepod.caching, rate control off). p1 healthy, versions bump every 1s. Expect: fresh band no fetch, SWR serves stale + refresh, version lag bounded by `M+W+1`.
2. `caching-sie-503` (spicepod.caching). t=30 p2 -> 50% 503 + 20ms latency; t=90 recover. Expect SIE serves stale within `E`; SWR refreshes fail silently; d1 unaffected.
3. `caching-sie-timeout` (spicepod.caching, MANDATORY). t=30 p2 -> `hang 5000ms` with `client_timeout=2s`; t=120 recover. Expect timeout classified as error, SIE serves stale within `E`, connector timed out (evidence: origin hang log + ~2s refresh latency).
4. `caching-sie-expiry` (spicepod.caching, `caching_stale_if_error: "10s"`). Long error window > `E`. Expect stale served until `E`, then errors propagate (fail closed).
5. `ratecontrol-sre` (spicepod.ratecontrol.sre, K=2.0). Offered QPS above ceiling; t=30 p2 90% 503; t=120 recover. Expect admission ≈ SRE formula over the decaying window and drops well below 1000, p2 upstream arrivals capped, p1 clean.
6. `ratecontrol-cooldown` (spicepod.ratecontrol.sre). p2 returns 429 + `Retry-After: 2`. Expect retry-after metrics move and a ~2s arrival gap.
7. `ratecontrol-ietf-headers` (spicepod.ratecontrol.sre). p2 sends only `RateLimit`/`RateLimit-Policy`. Expect NO admission change today (assertion flips when #14136 lands).
8. `topology-isolation` (spicepod.composition). Fault p2 only. Expect both p2 datasets throttled/stale, p1 untouched; one shared p2 limiter series.
9. `composition-sie-absorbs-while-backoff` (spicepod.composition). p2 timeout window. Expect: SIE keeps responses `ok=true` STALE while the controller backs off and upstream arrivals collapse — the two features cooperate (cache absorbs user-facing hits while the controller protects the origin).
10. `slow-confirmation` (spicepod.slow, real durations e.g. max-age=60s, swr=120s, sie=1h). One long run mirroring scenario 2/3 to confirm the tiny-window results hold at realistic timescales.

(An `overshoot-recovery` scenario and an AIMD-strategy `ratecontrol-aimd` scenario were dropped from this catalog: #14143 ships one admission-coefficient strategy, not a separate AIMD control law, so there is no additive-increase dynamic to re-trip and test. See `docs/dev` history / PR discussion if AIMD-shaped ceiling behavior is reintroduced.)

## 8. Duration scaling

- Default fast profile: `caching_ttl=3s`, `caching_stale_while_revalidate_ttl=6s`, `caching_stale_if_error=60s`, `client_timeout=2s`, `bump_interval=1s`. A full scenario runs in ~3-5 minutes.
- One slow-confirmation run (scenario 12) uses real durations.
- Guardrail: the run harness prints a warning if any window exceeds a threshold (e.g. `caching_ttl > 30s`) outside the explicit `slow` profile, so nobody benchmarks with large windows by accident and calls a 20-minute idle a pass.

## 9. Tech-stack recommendation

- Origin servers: FastAPI + Uvicorn. Rationale: async request handling (needed for realistic latency/hang without blocking other requests), trivial JSON endpoints, and a clean control API; `hang` is a bare `await asyncio.sleep`. For the `refuse` mode, drop to a raw asyncio socket listener that closes on connect (FastAPI cannot easily refuse mid-connect), so ship one small stdlib-socket helper alongside FastAPI.
- Load generator: Python `asyncio` + `httpx.AsyncClient`. Rationale: open-loop pacing with cancellation is straightforward in asyncio, and `httpx` gives per-request timing and header access; it queries spiced over HTTP SQL (`POST /v1/sql`).
- Scenario driver, scraper, oracle: plain Python stdlib + `httpx` + `pandas` for correlation. Rationale: no extra services; `pandas` makes the time-aligned joins and the single correlation plot (matplotlib) cheap.
- Orchestration: `docker-compose` for one-command bring-up (`make harness-up`), with a `make harness-run SCENARIO=...` target that runs the driver+load-gen+scraper and writes the run directory. Rationale: reproducible topology, fixed hostnames (`p1.local`, `p2.local`) that become the origin metric labels, and easy CI later.

## 10. Run hygiene

Each run creates `runs/<UTC-timestamp>-<scenario>/` containing: the resolved spicepod(s), the spiced git commit (`spice version` + `git rev-parse HEAD`), `timeline.yaml`, all origin request logs, the load-gen records, `metrics.csv`, driver events, `assertions.json`, `correlation.png`, and a `manifest.json` (seed, durations, image digests). The oracle exit code is the run's pass/fail.

## 11. Phased build order

1. Phase 0 — skeleton and oracle spike. One origin (FastAPI) with `/data` + `/control` + request log; one caching dataset; a trivial load generator that reads `version`. Confirm the version oracle end-to-end AND empirically test whether `SELECT _fetched_at` works (Section 2.2). Deliverable: a passing `caching-swr-basic`.
2. Phase 1 — fault modes + driver + clock. Add 503/429/refuse/hang/latency, the control API, and the shared-clock driver. Deliverable: `caching-sie-503` and `caching-sie-timeout` pass.
3. Phase 2 — metrics scraper + rate-control assertions. Add the scraper and the per-origin adaptive/cooldown checks. Deliverable: `ratecontrol-sre`, `ratecontrol-cooldown`, `ratecontrol-ietf-headers`.
4. Phase 3 — topology + composition. Second origin, two p2 datasets, isolation/sharing checks, and the composition scenario.
5. Phase 4 — hygiene, compose, slow run, correlation plot. One-command bring-up, run directories, the guardrail warning, and scenario 12.

Each phase is independently useful: Phase 0 already validates the single most load-bearing assumption (the payload oracle).

## 12. Open questions and risks (need a decision)

1. `_fetched_at` visibility — UNCONFIRMED whether `SELECT *` or `SELECT _fetched_at` surfaces it to a client query. Phase 0 must settle this. If it is not selectable, the harness relies solely on the origin `version` oracle (acceptable, but it removes the independent cross-check). Decision needed only if Phase 0 shows the version oracle alone is insufficient.
2. File format for `/data` — the plan assumes `file_format: json`. Confirm the HTTP connector schema-infers the origin JSON as intended, and that a caching accelerator over it round-trips the `version` column. Fallback: serve CSV or a single-column payload.
3. Rate control config location — RESOLVED: the adaptive knob is a `runtime.params` key (`http_adaptive_rate_control`, `enabled`/`disabled`), applied per origin, not a per-dataset param; no dataset-level override exists on the merged `14136`/#14143 surface. Also RESOLVED: #14143 ships a single admission-coefficient (SRE-style) strategy, not an AIMD/SRE pair this plan originally assumed — the `ratecontrol-aimd` scenario, its spicepod, and the AIMD-specific harness assertions were removed; `ratecontrol-sre`, `ratecontrol-cooldown`, and `ratecontrol-ietf-headers` all now share `spicepod.ratecontrol.sre.yaml`.
4. Ceiling semantics — the SRE ceiling derives from the max of configured rps/rpm/concurrency, else 100. The harness must set an explicit `http_requests_per_second_limit` so the ceiling is known; otherwise assertions must target the default 100.
5. IETF header behavior may flip mid-project — #14136 may land `RateLimit`/`RateLimit-Policy` handling during harness development. Scenario 8's expected outcome must be a config flag, not a hardcoded assumption.
6. Metric label stability — assertions key on the `origin` label value (`rate_control_key(base_url)`). Confirm the exact string (host:port vs host) so oracle filters match; pin it from a first live scrape rather than assuming.
7. Branch volatility — both features live on branches (SIE at `14126-stale-if-error`, rate control uncommitted on `14136-adaptive-rate-control`). Metric names and params can change before merge. Treat Section 2.3 and 4.3 names as "verify at build time"; the harness should read metric names from a small config file, not hardcode them, so a rename is a one-line change.
