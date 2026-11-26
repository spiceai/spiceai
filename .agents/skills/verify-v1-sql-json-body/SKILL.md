---
name: verify-v1-sql-json-body
description: Drive a real `spiced` over HTTP to prove how `POST /v1/sql` treats JSON and text bodies across Content-Type shapes — missing `parameters`, `parameters: null`, `application/json; charset=utf-8`, `Application/JSON`, parameterized JSON, and `text/plain`. Use when changing `crates/runtime/src/http/v1/query.rs` or `/v1/sql` JSON body handling.
---

# Verify `/v1/sql` JSON body handling

Unit tests in `crates/runtime/src/http/v1/query.rs` cover Content-Type matching and JSON deserialization. This skill proves the route a user calls, on a built `spiced`, the way curl, the SDKs, and .NET `HttpClient.PostAsJsonAsync` (which always sends `charset=utf-8`) call it.

`SELECT 1` needs no dataset.

## Build

```bash
CC=cc CXX=g++ cargo build -p spiced --no-default-features
```

The HTTP SQL endpoint is always linked, so no network or extra features are needed.

## Run

```bash
.agents/skills/verify-v1-sql-json-body/helpers/verify-v1-sql-json-body.sh target/debug/spiced
```

The helper:

1. Writes a Spicepod with no datasets under a fresh `mktemp -d` work directory (override with `WORK_DIR`, which must be empty or new).
2. Refuses to run if anything already answers on `HTTP_ADDR`, then starts only this `spiced` on `HTTP_ADDR` (default `127.0.0.1:18392`) plus side ports, logging to `$WORK_DIR/spiced.log`, and records its pid.
3. Waits for `GET /v1/ready` to be `200` (90s timeout). An open port is not ready.
4. Sends each body shape and prints one line per case: `case | HTTP status | body`.
5. Stops the pid it started (never `pkill`) and removes the work directory it created unless `KEEP=1`. A `WORK_DIR` you pass is never removed.

To point it at an already-running instance, pass `--url http://host:port` instead of a binary; it then skips launch and teardown.

## Expected after the `/v1/sql` JSON body fix

| case | expected |
|---|---|
| `{"sql":"select 1 as one"}` with `Content-Type: application/json` | 200 `[{"one":1}]` |
| `{"sql":"select 1 as one","parameters":null}` with `application/json` | 200 `[{"one":1}]` |
| `{"sql":"select 1 as one","parameters":[]}` with `application/json; charset=utf-8` | 200 `[{"one":1}]` |
| `{"sql":"select 1 as one"}` with `Application/JSON` | 200 `[{"one":1}]` |
| `{"sql":"select $1 as one","parameters":[1]}` with `application/json` | 200 `[{"one":1}]` |
| `{"sql":"select :foo + 1 as the_answer","parameters":{"foo":41}}` with `application/json` | 200 `[{"the_answer":42}]` |
| `select 1 as one` with `Content-Type: text/plain` | 200 `[{"one":1}]` |

Before the fix, the SQL-only JSON body returns 400 `Invalid JSON: missing field \`parameters\` …`, and the charset / `Application/JSON` cases return 400 `ParserError("Expected: an SQL statement, found: {")` because the body is treated as SQL. Parameterized JSON and `text/plain` are unchanged by the fix.

## Proof

Run the helper against the binary built before the change and the one after, and keep both outputs. A compile or the unit tests alone are not proof.
