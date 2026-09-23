# HTTP cache / rate-limit harness

A testing harness for caching accelerated datasets on `spiced`. Applies load through spiced onto
a HTTP server and asserts various properties of the cache. Tiers of testing.
 - Phase 0: Basic single origin & dataset. Asserts result freshness, bounds
 of caching TTL, and reduces load on origin (i.e. acts like a cache). 
 - Phase 1: Satisfies RFC 5861 stale-if-error mechanisms. 
 - Phase 2: Measures effectiveness of adaptive rate controls, both additive increase, multplicative decrease, and sucess-based throttling.
 
```ascii
  [ load gen ] ---> [ spiced :8090 ] ---> [ HTTP origin 1 :9001 ]
                         ^                    GET  /data
  [ Oracle ] ------------+------------------> POST /control
                   verify invariants          GET  /stats
                                          [ HTTP origin 2 :9002 ]
```

## Components

- `origin/server.py`: HTTP origin
- `spicepod/spicepod.*.yaml`: spicepods for stages.
- `loadgen/run_phase*.py`: 
- `loadgen/metrics_scraper.py`:  Prometheus `/metrics` poller used by
  Phase 2. Can run standalone with `--endpoint`.
- `loadgen/metrics_config.json` — the metric names Phase 2's oracle keys on,
  in one place (see "Metric names — verify, don't assume" below).
- `loadgen/multi_key.py`: the shared `request_query` key set, a seeded picker,
  and the `WHERE ... AND request_query = '<key>'` SQL builder used by all three
  load generators, so distinct keys hit distinct upstream requests / cache
  entries against one origin.
- `run_phase0.sh`, `run_phase1.sh`, `run_phase2.sh`: one-command
  orchestration per phase.
- `loadgen/report.py`: renders any run directory (any phase) into one static
  HTML report — QPS at the HTTP origin(s) and at spiced (each also broken
  down per status), latency, staleness/freshness, and (Phase 2) admission
  coefficient / effective limit, as inline SVG charts. No server, no
  third-party dependency. See "View a run" below.

Every dataset sets `request_query_filters: enabled` and declares a
`request_query` metadata column, so `WHERE request_query = '<key>'` pushes down
as a per-key upstream request (and cache key). Without the declared column the
planner rejects the filter as an unknown column.

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

# heal it back to normal:
curl -X POST http://127.0.0.1:9001/control -H 'Content-Type: application/json' \
  -d '{"id":"healthy","mode":"healthy","error_rate":0.0}'
```

## Run it

```bash
cd test/harness/http_cache_ratelimit
pip install -r requirements.txt

# Phase 0 (steady-state freshness):
QPS=10 DURATION_S=20 ./run_phase0.sh

# Phase 1 (stale-if-error scenarios):
./run_phase1.sh caching-sie-timeout     # MANDATORY
./run_phase1.sh caching-sie-refuse
./run_phase1.sh caching-sie-503
./run_phase1.sh caching-sie-expiry

# Phase 2 (adaptive rate control) needs a spiced built with the
# `rate-control` feature
./run_phase2.sh --probe ratecontrol-sre
SPICED_BIN=/path/to/spiced ./run_phase2.sh ratecontrol-sre
```

Artifacts land in `$RUN_DIR`. Notably:
- `samples.csv` one row per query
- `driver_events.csv`: fault steps on the shared clock.
- `assertions.json` (the verdict). 

## View a run

```bash
python loadgen/report.py --run-dir "$RUN_DIR"     # writes $RUN_DIR/report.html
```

Works on any phase's run directory, including an old one you didn't just
run — it only reads the CSV/JSON a run already wrote. Open the file
directly in a browser; nothing to serve.

## Assertions
### Phase 0
 - `all_queries_returned_data`: every read returned a row, no errors.
 - `cache_absorbs_load`: origin /data fetches ≪ query count.
 - `fetch_count_near_max_age_cadence`: fetches ≈ duration / max_age, not one per query.
 - `cache_never_ahead_of_origin`: version_seen ≤ hwm always (correctness).
 - `staleness_bounded`: max lag ≤ ceil((max_age + swr)/bump) + margin.
 - `served_version_monotonic`: version_seen never regresses.

### Phase 1 
 - `cache_never_ahead_of_origin`
 - `warmup_served_data`: the cache served data during the healthy warmup window.
 - `sie_serves_stale_through_error`: at least one stale row served, never empty/error, throughout the SIE window.
 - `sie_stale_version_is_frozen_and_below_hwm`: the served stale version stops advancing and stays below the origin's hwm while frozen.
 - `connector_timed_out_not_full_hang`: (timeout scenario only) the connector aborts at `client_timeout`, not the full hang duration.
 - `recovery_resumes_freshness`: fresh serving resumes once the origin recovers.

 ### Phase 2
 - `backoff_on_failure`: p2's admission coefficient drops well below 1000‰ during the fault window; p2's upstream arrival rate falls below the offered rate. (No separate throttled-request counter exists — #14143 exposes only the admission-coefficient and effective-limit gauges.)
 - `sre_shape`: admission tracks min(1, (K·accepts+1)/(requests+1)) over the ~10s decaying window, within tolerance.
 - `cooldown_retry_after`: rate_limit_retry_after_* metrics move and p2's arrival log shows a gap ≈ the advertised Retry-After duration.
 - `p1_isolation`: the healthy origin (p1) stays at 1000‰ admission throughout, proving rate control is per-origin.

## Phase 1 scenarios and what the oracle asserts
Process:
 - Warms the cache
 - Injects a fault at `t = warmup_s` for `fault_s`. 
 - Recovers at `warmup_s + fault_s`

The stale if error window is `[warmup_s + max_age + swr, fault_end]`.

| scenario | fault | expected on the SIE feature | prebuilt v2.3.1 |
|---|---|---|---|
| `caching-sie-timeout` | `hang` > `client_timeout` | serve stale (send error) | **PASS** |
| `caching-sie-refuse` | connection reset | serve stale (send error) | **PASS** |
| `caching-sie-503` | HTTP 503 | serve stale (RFC 5861) | **BLOCKED** — returns empty |
| `caching-sie-expiry` | long error window | stale then fail-closed at the bound | **PENDING** — duration SIE not in binary |

Assertions
 - `cache_never_ahead_of_origin`: Cache is always older than server.
 - `sie_serves_stale_through_error`: No errors during SIE window.
 - `warmup_served_data`, `sie_serves_stale_through_error`
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

## Phase 2 — adaptive rate control

Puts a real `spiced` in front of both origins, breaks p2 on a schedule, and
lets the oracle decide — from scraped metrics and both origins' request
logs — whether the per-origin rate controller backs off the failing origin
and leaves the healthy one alone, using #14143's **Google SRE** client-side
throttling strategy (the only adaptive strategy it ships — see the status
note below on an earlier AIMD-shaped run against a prior branch build).

### Requirements

`adaptive_rate_control` needs a `spiced` built with the **`rate-control`**
cargo feature. It is **not** in the default build and **not** in the
prebuilt `~/.spice/bin/spiced` — building without it means the metrics
never appear and every Phase 2 run is BLOCKED.

```bash
# from a checkout that has the adaptive_rate_control code (e.g. via
# `make install SPICED_NON_DEFAULT_FEATURES='...,rate-control'`, or:
cargo build --release -p spiced --features "rate-control,duckdb"
```

Point the harness at it with `SPICED_BIN` (it otherwise defaults to
`~/Github/spiceai/target/release/spiced`, then `~/.spice/bin/spiced`).

### Run a scenario

```bash
# ALWAYS probe first on a new binary: start everything, scrape /metrics once,
# and print the exact metric names + origin label the runtime exposes.
SPICED_BIN=/path/to/spiced ./run_phase2.sh --probe ratecontrol-sre

# Then score a scenario. Exit code is the verdict: 0 PASS, 1 FAIL, 2 BLOCKED.
SPICED_BIN=/path/to/spiced ./run_phase2.sh ratecontrol-sre
```

`run_phase2.sh` starts origin p1 (:9001) and p2 (:9002), starts `spiced`
(SQL :8090, metrics :9090), drives the scenario, tears everything down, and
returns the oracle verdict.

| scenario | p2 fault | what the oracle checks | pod |
|----------|----------|------------------------|-----|
| `ratecontrol-sre` | 90% 503 | admission tracks `min(1, (K·accepts+1)/(requests+1))` reconstructed from p2's arrival log, within `--sre-tolerance` | `ratecontrol.sre` |
| `ratecontrol-cooldown` | 429 + `Retry-After: 2` | `rate_limit_retry_after_*` metrics move and p2 arrivals show a ≈2 s gap | `ratecontrol.sre` |
| `ratecontrol-ietf-headers` | 200 + `RateLimit`/`RateLimit-Policy` only | admission does **not** change (advertised quota not honored yet, `TODO(#14136)`) | `ratecontrol.sre` |

Each scenario runs `warmup → fault → recovery` (default 15 s / 60 s / 45 s, so
~2 min). p1 stays healthy throughout; only p2 is faulted, which is how
cross-origin isolation is proven. SRE with K=2 only throttles once the success
ratio falls below `1/K = 0.5`, so that scenario faults at 90%.

### Reading the results

Artifacts land in `RUN_DIR` (default `/tmp/http_cache_phase2_run/<scenario>/`):

| file | what it gives you |
|------|-------------------|
| `assertions.json` | the verdict, per-assertion pass/fail with evidence, and a `config` + `evidence` block (see below) |
| `metrics.csv` | every kept scrape: `scrape_epoch_ms, t_rel_s, metric_name, origin, value` |
| `samples.csv` | one row per SQL query: `t_send_rel_s, dataset, origin_name, phase, latency_ms, http_status, ok, rows, version_seen, error_kind` |
| `origin_p1.jsonl`, `origin_p2.jsonl` | origin arrival logs — the ground truth for "did the request actually reach upstream" |
| `driver_events.csv` | the fault steps, stamped on the shared clock |
| `discovered_metrics.txt` | the exact series names the live `/metrics` exposed (from any run, not just `--probe`) |
| `spiced.log`, `spiced.version`, `spiced.commit` | the binary and its output |

The console prints a per-scrape trace of p2's admission coefficient and
effective limit, the driver timeline, and the assertion table. The `evidence`
block in `assertions.json` is the quick read — e.g.
`p2_admission_min_permille_fault`, `p2_effective_limit_min_fault`,
`p2_upstream_arrivals_fault` vs `p2_offered_fault`, and
`p1_admission_min_permille_all`.

**Verdict codes:** `0` PASS (all assertions held) · `1` FAIL (an assertion
failed on a binary that does expose the feature) · `2` BLOCKED (no adaptive
series was ever seen — wrong build, dataset didn't register, or the metric
names don't match — read `blocked_reason` and `discovered_metrics.txt`).

### Metric names — verify, don't assume

The oracle keys on metric names and the `origin` label. Both can drift on
the branch, so they live in
[`loadgen/metrics_config.json`](loadgen/metrics_config.json), matched by
**prefix** (an exporter-added `_total` or unit suffix is absorbed). A
rename is a one-line edit there, not a code change.

Confirm them against the live endpoint before trusting a run:

```bash
SPICED_BIN=/path/to/spiced ./run_phase2.sh --probe ratecontrol-sre
# → prints kept series + per-origin values; also writes
#   <RUN_DIR>/metrics_probe.txt and metrics_raw_ratecontrol.txt
```

Or scrape directly while a run is up:

```bash
curl -s localhost:9090/metrics | grep -E 'adaptive_rate_control|rate_control_|rate_limit_retry_after'
```

The `origin` label is a full base URL (e.g. `http://127.0.0.1:9002`); the
oracle maps p1/p2 to it by port.

### Configuration

Everything is env-var driven. `run_phase2.sh` knobs:

| var | default | meaning |
|-----|---------|---------|
| `SPICED_BIN` | branch build, then `~/.spice/bin/spiced` | the binary to test |
| `P1_PORT` / `P2_PORT` | `9001` / `9002` | origin ports |
| `HTTP_PORT` / `METRICS_PORT` | `8090` / `9090` | spiced SQL / Prometheus ports |
| `RUN_DIR` | `/tmp/http_cache_phase2_run/<scenario>` | artifact directory |
| `BUMP_INTERVAL_S` | `1.0` | how often each origin's `version` ticks |

Load, timeline, and thresholds (passed through to `loadgen/run_phase2.py`):

| var | default | meaning |
|-----|---------|---------|
| `WARMUP_S` / `FAULT_S` / `RECOVERY_S` | `15` / `60` / `45` | timeline windows (seconds) |
| `P1_QPS` / `P2_QPS` | `10` / `30` | offered query rate per origin (drive p2 above the ceiling; p1 stays >=10 so its charts have enough points/sec to read) |
| `RPS_LIMIT` | `20` | the ceiling the pods set (`http_requests_per_second_limit`) |
| `SRE_K` | `2.0` | SRE hyperparameter; must match the SRE pod |
| `SCRAPE_INTERVAL_S` | `1` | `/metrics` poll cadence |
| `REQUEST_TIMEOUT_S` | `15` | per-SQL-query client timeout |
| `MAX_WORKERS` | `128` | open-loop sender pool size |

Assertion thresholds are `run_phase2.py` flags:
`--admission-drop-permille` (800), `--recovery-permille` (950),
`--p1-full-permille` (1000), `--sre-tolerance` (0.2),
`--cooldown-gap-min-s` (1.5). Run
`.venv/bin/python loadgen/run_phase2.py --help` for the full list.

To run the load generator by hand against an already-running stack, point it
with `SPICED_SQL_URL`, `METRICS_ENDPOINT`, `P{1,2}_CONTROL_URL`,
`P{1,2}_STATS_URL`, `P{1,2}_REQUEST_LOG`, and `OUT_DIR`.

Datasets are plain (non-accelerated) federated HTTP datasets, so **one SQL
query is one upstream request** — the cleanest signal for the SRE
math. Adaptive control is a *modifier* on a static limit, so the pods set
`http_requests_per_second_limit: 20`; without a static limit the dataset is
rejected at load.

### Phase 2 status (historical, superseded): an earlier `14136` build exercised AIMD-shaped dynamics

**This section documents a run against a *prior* revision of
`14136-adaptive-rate-control`, kept as evidence rather than deleted, not
as the current scenario.** #14143's PR description ships one strategy
(Google SRE client-side throttling, `http_adaptive_rate_control`
`enabled`/`disabled` + `_failure_threshold` + `_window`); no AIMD mode is
in scope there, and the code on that branch (`crates/data-http-rate-control`)
has no `aimd`-named path. The `ratecontrol-aimd`/`overshoot-recovery`
scenarios and `spicepod.ratecontrol.aimd.yaml` were removed from this
harness accordingly. **Flagging for the PR author**: the run below shows a
real, separately-tracked `effective_limit` gauge halving under fault and
recovering additively against an earlier build of that same branch — worth
confirming whether that AIMD-shaped ceiling dynamic was simplified away
before #14143's current description, or whether it still exists alongside
the SRE admission coefficient and this doc's description of "one strategy"
is incomplete.

Verified against `spiced` built from `14136-adaptive-rate-control` (an
earlier revision than #14143's current description) with the
`rate-control` feature. `--probe` confirmed every
`adaptive_rate_control_*`/`rate_control_*` series exists under a
**`dataset_http_`** prefix (see "Metric names" above), then a full scored
run:

```
[PASS] cache_never_ahead_of_origin: 0 samples returned a version above the origin high-water mark
[PASS] warmup_load_flowing: 494/504 warmup queries returned rows
[PASS] p2_admission_drops_during_fault: min admission_coefficient_permille[p2] during fault = 50.0 (need < 800.0; 1000 = admit all)
[PASS] p2_effective_limit_multiplicative_decrease: min effective_limit[p2] during fault = 1.0 (need <= ceiling/2 = 10.0; ceiling = 20.0)
[PASS] p2_throttled_total_increases: throttled_total[p2] delta over fault = 1031.0 (need > 0)
[PASS] p2_arrivals_capped_below_offered: p2 upstream arrivals during fault = 1201 (20.0/s) vs offered ~1800 (30/s); the limiter capped the origin
[PASS] p2_recovers_admission_and_limit: end-of-recovery admission[p2] = 1000.0 (need >= 950.0), effective_limit[p2] = 20.0 (need >= 15.0)
[PASS] p1_admission_stays_full: min admission_coefficient_permille[p1] over the whole run = 1000.0 (need >= 1000.0; p1 never faulted)

VERDICT: PASS  (exit 0)
```

8/8 assertions PASS: AIMD's multiplicative decrease is real (effective
limit dropped to 1, from a ceiling of 20), the throttle actually shielded
the origin (p2 arrivals capped at ~20/s against an offered ~30/s), the
limit and admission both fully recover after the origin heals, and p1
(never faulted) stayed at 1000‰ admission the entire run — per-origin
isolation holds.

### Phase 2 status (current): `ratecontrol-sre` scored against the SRE-only build

Verified against `~/.spice/bin/spiced` v2.4.0-unstable-build.1450ccd8d0
(has the shipped `http_adaptive_rate_control`/`_failure_threshold`/`_window`
params baked in). `--probe` confirmed the two adaptive gauges exist and no
`throttled_total`/`throttle_wait_duration_ms` series does — see 2.3 — then
a full scored run:

```
[PASS] cache_never_ahead_of_origin: 0 samples returned a version above the origin high-water mark
[PASS] warmup_load_flowing: 465/538 warmup queries returned rows
[PASS] p2_admission_drops_during_fault: min admission_coefficient_permille[p2] during fault = 272.0 (need < 800.0)
[PASS] sre_admission_matches_formula: mean|predicted-observed| admission over fault (2nd half) = 0.001 (need <= 0.2)
[PASS] p2_recovers_admission: end-of-recovery admission[p2] = 1000.0 (need >= 950.0)
[PASS] p1_admission_stays_full: min admission[p1] over the whole run = 1000.0 (need >= 1000.0)

VERDICT: PASS  (exit 0)
```

6/6 assertions PASS: admission drops sharply during the fault window and
tracks the SRE formula `min(1, (K·accepts+1)/(requests+1))` within 0.001 of
predicted, fully recovers to 1000‰ after the origin heals, and p1 (never
faulted) stays at 1000‰ throughout — per-origin isolation holds.

`ratecontrol-cooldown` and `ratecontrol-ietf-headers` have not been scored
against the current SRE-only build yet — treat them as PENDING until their
own `assertions.json` is pasted in here.

Branch facts confirmed by reading `crates/data-http-rate-control` and
`crates/data_components/src/rate_limit/adaptive.rs` on
`14136-adaptive-rate-control`, and by a live `/metrics` scrape
(re-verify if #14143 changes further before merge):

- Every `adaptive_rate_control_*`/`rate_control_*`/`rate_limit_retry_after_*`
  series is exposed with a **`dataset_http_` prefix**
  (e.g. `dataset_http_adaptive_rate_control_effective_limit`), not bare —
  the design doc's plan text omits this prefix; `metrics_config.json` has
  the confirmed, prefixed names. If a future scrape shows a rename, fix it
  there only.
- There is no `DEFAULT_ADAPTIVE_CEILING = 100` fallback — a static limit is
  required (`ensure_adaptive_has_static_limit`); the ceiling the controller
  grows back toward is the max of the configured rps/rpm/concurrency
  limits. The pods above pin `http_requests_per_second_limit: 20`.
- The `origin` metric label is the full `scheme://host:port` base URL, not
  bare `host:port` — confirmed live (`origin="http://127.0.0.1:9001"`).
- The structured IETF `RateLimit`/`RateLimit-Policy` header is not parsed
  yet (only `Retry-After`/`RateLimit-Reset` are), matching the
  not-yet-honored behavior in `docs/dev/http_cache_ratelimit_harness_plan.md`
  Section 2.3 — `ratecontrol-ietf-headers` asserts admission does NOT
  change today; flip that assertion when the header support lands.
- The dataset-level override key is **`adaptive_rate_control`** (no
  `http_` prefix); `http_adaptive_rate_control` is specifically the
  `runtime.params` key. Both exist; the spicepods above only use the
  `runtime.params` form.
