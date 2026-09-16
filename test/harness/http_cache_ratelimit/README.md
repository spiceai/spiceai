# HTTP cache / rate-limit harness — Phase 0

A closed test rig that measures how a Spice.ai `refresh_mode: caching`
accelerator behaves in front of a live, changing HTTP origin. Phase 0 is
the smallest useful slice: one origin, one caching dataset, and a load
generator that decides — from evidence — whether the cache serves fresh
data, bounds staleness, and shields the origin from load.

```
  loadgen/run_phase0.py            spiced (:8090)              origin (:9001)
  ------------------------         ----------------           --------------
  every 1/QPS seconds:             dataset d1                 GET /data
    GET  /stats  -> hwm     --->   duckdb, refresh_mode:  --> {"id":1,
    POST /v1/sql (filtered)        caching (max_age 3s,       "version":N,
      SELECT version, _fetched_at    swr 6s)                   ...}
      FROM d1 WHERE origin='p1'                               version N bumps
    compare version_seen vs hwm                              +1 every 1s
```

## Components

- `origin/server.py` — FastAPI origin. Serves one row whose integer
  `version` increases on a fixed cadence (`ORIGIN_BUMP_INTERVAL`, default
  1s). `GET /stats` reports the high-water `version` and a `/data` request
  counter; every `/data` GET is appended to `ORIGIN_REQUEST_LOG`.
- `spicepod/spicepod.caching.yaml` — one dataset `d1`, DuckDB accelerator,
  `refresh_mode: caching` (`caching_ttl: 3s`, `caching_stale_while_revalidate_ttl: 6s`).
- `loadgen/run_phase0.py` — the load generator **and** the freshness
  oracle. Drives queries, records per-response samples, and emits a
  pass/fail verdict.
- `run_phase0.sh` — one-command orchestration: start origin + spiced, wait
  until ready, run the load, tear down.

## Run it

```bash
cd test/harness/http_cache_ratelimit
python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt   # once
QPS=10 DURATION_S=20 ./run_phase0.sh
```

Artifacts land in `$RUN_DIR` (default `/tmp/http_cache_phase0_run`):
`samples.csv` (one row per query), `assertions.json` (the verdict),
`spiced.log`, `origin.stdout.log`. Exit code is the oracle verdict
(0 = all assertions pass).

## What the oracle asserts

For each query it samples the origin high-water version (`hwm_at_send`)
just before reading `version` back through the cache, so
`lag_versions = hwm_at_send - version_seen`.

| assertion | claim |
|---|---|
| `all_queries_returned_data` | every read returned a row (no errors) |
| `cache_absorbs_load` | origin `/data` fetches ≪ query count |
| `fetch_count_near_max_age_cadence` | fetches ≈ `duration / max_age`, not one per query |
| `staleness_bounded` | `max lag ≤ ceil((max_age + swr)/bump) + margin` |
| `cache_never_ahead_of_origin` | `version_seen ≤ hwm` always (correctness) |
| `served_version_monotonic` | `version_seen` never regresses |

## Three findings that shape the config (verified against prebuilt `spiced` v2.3.1, `f3ca9d17dc`)

1. **A caching HTTP dataset needs the dynamic-API provider, not the
   listing connector.** `file_format: json` alone routes to the
   object-store listing connector, which issues WebDAV `PROPFIND` /
   `Range` requests a dynamic endpoint answers with `405` (or an empty
   listing → empty schema). Setting `allowed_request_paths` selects the
   dynamic JSON API provider, which does a plain `GET` on the base URL.

2. **`refresh_mode: caching` only refreshes on a *filtered* query.**
   `CachingAccelerationScanExec::execute` returns cached rows directly
   when the scan has no filters (`self.filters.is_empty()`), skipping the
   staleness check and the source fetch entirely. A cold, unfiltered
   `SELECT version FROM d1` therefore returns `[]` with zero origin hits,
   silently. The load generator always queries **with a `WHERE`**.

3. **Filter on a string column, not an integer.**
   `... WHERE id = 1` (or any numeric-column predicate) fails with
   `Internal error: Could not create ExprBoundaries: ... col_index has
   gone out of bounds`. `... WHERE origin = 'p1'` works and triggers the
   fetch. The generator uses the string filter.

The `_fetched_at` cache-timestamp column **is** selectable and is
surfaced by `SELECT *`; it advances in lockstep with each cache refresh,
so it doubles as an independent freshness marker in `samples.csv`.

## Example evidence (QPS=10, 20s, bump 1s, max_age 3s, swr 6s)

```
queries=200 ok=200 errors=0
origin HWM 2 -> 22; origin /data fetches during run = 10 (log window = 10)
max lag = 2 versions (bound 12)
```

200 reads, 10 origin fetches (20:1 absorption), lag never past 2 — all six
assertions pass.
