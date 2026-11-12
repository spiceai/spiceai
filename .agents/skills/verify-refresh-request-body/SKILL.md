---
name: verify-refresh-request-body
description: Drive a real `spiced` over HTTP to prove how `POST /v1/datasets/{name}/acceleration/refresh` (and any other route using the shared `OptionalJson` extractor) treats its optional JSON body across header shapes - no body, an empty body with a JSON or form `Content-Type`, whitespace, `{}`, valid overrides, a JSON body under a form `Content-Type`, malformed JSON, and an unknown `refresh_mode`. Use when changing `crates/runtime/src/http/optional_json.rs`, the refresh handler, or HTTP body extraction.
---

# Verify the refresh request body

Unit tests in `crates/runtime/src/http/optional_json.rs` cover the extractor alone. This skill proves the route a user calls, on a built `spiced`, the way curl, the SDKs, and a proxy that adds `Content-Type: application/json` to a body-less POST call it.

## Build

```bash
CC=cc CXX=g++ cargo build -p spiced --no-default-features
```

The file connector and Arrow acceleration are always linked, so no network or extra features are needed.

## Run

```bash
.agents/skills/verify-refresh-request-body/helpers/verify-refresh-body.sh target/debug/spiced
```

The helper:

1. Seeds a local CSV and an Arrow-accelerated `file:` dataset `events` under a fresh `mktemp -d` work directory (override with `WORK_DIR`).
2. Starts only this `spiced` on `HTTP_ADDR` (default `127.0.0.1:18391`) plus side ports, logging to `$WORK_DIR/spiced.log`, and records its pid.
3. Waits for `GET /v1/ready` to be `200` (90s timeout). An open port is not ready.
4. Sends each body shape and prints one line per case: `case | HTTP status | body`.
5. Stops the pid it started (never `pkill`) and removes the work directory it created unless `KEEP=1`. A `WORK_DIR` you pass is never removed.

To point it at an already-running instance, pass `--url http://host:port` instead of a binary; it then skips launch and teardown. Use a dataset name with `DATASET=` if it is not `events`.

## Expected after the `OptionalJson` fix

| case | expected |
|---|---|
| no body, no Content-Type | 201 |
| empty body, `Content-Type: application/json` | 201 |
| empty body, `Content-Type: application/x-www-form-urlencoded` | 201 |
| whitespace body, `Content-Type: application/json` | 201 |
| `{}` | 201 |
| `{"refresh_mode":"full"}` | 201 |
| `{"refresh_mode":"bogus"}` under `application/x-www-form-urlencoded` (`curl -d`) | 415, unchanged: a form or `text/plain` body is a cross-site simple request and must not carry overrides |
| `{"refresh_mode":"bogus"}` with no `Content-Type` | 201, unchanged: the body is ignored, as `Option<Json<T>>` did |
| malformed JSON `{not json` | 400 `Failed to parse the request body as JSON` |
| `{"refresh_mode":"bogus"}` | 422 (400 on runtimes that validate the mode later) |

Before the fix, the empty and whitespace bodies with `Content-Type: application/json` return 400 `Failed to parse the request body as JSON: EOF while parsing a value at line 1 column 0`, and an empty body under a form `Content-Type` returns 415. Every other row is unchanged by the fix.

## Proof

Run the helper against the binary built before the change and the one after, and keep both outputs. A compile or the unit tests alone are not proof.
