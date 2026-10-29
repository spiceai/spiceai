---
name: verify-spiced-file-connector
description: Drive a real `spiced` file-connector spicepod over HTTP SQL to prove CSV/TSV header name-binding and JSON/NDJSON schema-mismatch behavior (reorder, rename, restore) on federated and Arrow-accelerated datasets. Use when changing file-connector formats, CSV header projection, or file schema-drift handling.
---

# Verify `spiced` file-connector datasets

This skill drives a built `spiced` the way a user does: a file-connector spicepod, HTTP SQL, `/v1/ready`, `/v1/datasets`, and on-demand acceleration refresh. It does not treat unit tests or a compile as proof.

## Launch

Build a slim runtime (file connector and Arrow acceleration are always linked; extra connectors are not needed):

```bash
CC=cc CXX=g++ cargo build -p spiced --no-default-features
```

The binary is `target/debug/spiced`.

Create an isolated work directory (never the repo root, never a shared `.spice/`):

```bash
WORK_DIR="${WORK_DIR:-/tmp/spice-file-connector-verify}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18090}"
FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15051}"
METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19090}"
```

Seed `spicepod.yaml` plus `data/events.csv` and `data/events.jsonl` with the helper (it writes the baseline files and the pod):

```bash
.agents/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh seed --work "$WORK_DIR"
```

Start only this instance, with logs and a pidfile under `$WORK_DIR`:

```bash
.agents/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh launch \
  --spiced target/debug/spiced \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --flight "$FLIGHT_ADDR" \
  --metrics "$METRICS_ADDR"
```

`--spiced` and `--work` may be relative to the caller's directory. The helper canonicalizes both to absolute paths before changing to `$WORK_DIR` (so `logfile` / `pidfile` do not become `$WORK_DIR/$WORK_DIR/...`), and writes the binary path to `$WORK_DIR/spiced.binary`.

A second `launch` on the same `--work` is refused when that directory still tracks a live process (`spiced.pid` plus `spiced.identity` still match pid, executable, and starttime). The existing log, pidfile, and identity stay in place so `cleanup` can still stop the first instance. A dead or recycled PID may relaunch.

Exact process command the helper runs (cwd is `$WORK_DIR`, so `file:` paths resolve; `$SPICED` is already absolute):

```bash
cd "$WORK_DIR"
"$SPICED" --telemetry-enabled=false \
  --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" \
  .
```

The seeded spicepod uses absolute `file:` paths under `$WORK_DIR/data`.

**Ready is `/v1/ready`, not an open port.** Poll `GET http://$HTTP_ADDR/v1/ready` until the status is `200` and the body is `ready` (timeout 90s). A `503`/`not ready` means datasets have not finished loading. After a later refresh failure `/v1/ready` may return `503` again; that is expected and is not a launch failure.

Teardown is `cleanup` below. Do not `pkill -f spiced`.

## Doctor

Read-only check that this instance is the one the run started and is worth driving:

```bash
.agents/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh doctor \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --spiced target/debug/spiced
```

Doctor requires all of:

- `$WORK_DIR/spiced.identity` still names the pid in `$WORK_DIR/spiced.pid` (pid, `/proc/<pid>/exe`, `/proc/<pid>/stat` starttime). A missing identity, a reused PID, or a starttime/exe mismatch is refused even when `--spiced` is omitted
- when `--spiced` is supplied, that path must be the same executable the identity recorded
- `GET /v1/ready` is reachable (200 `ready` on a clean launch; after an expected Error the helper reports the 503 and still requires HTTP). A transport failure is refused.
- `GET /v1/datasets?status=true` lists `csv_fed`, `csv_acc`, `json_fed`, `json_acc`, `json_array_fed`, `json_array_acc`, `json_ptr_fed`, `json_ptr_acc`

Do not drive an instance that doctor did not start.

## Drive

HTTP SQL is the harness. `POST /v1/sql` with `Content-Type: text/plain` and `Accept: application/json`. Aliased sums are the observable:

```sql
SELECT sum(amount_cents) AS amount_cents, sum(user_id) AS user_id FROM <dataset>
```

On-demand refresh: capture a completion marker (`last_refresh`, `error_message`, and `runtime.task_history` `acceleration_refresh` count) first, then `POST /v1/datasets/<name>/acceleration/refresh`. A transport error, a non-2xx, or an invalid `/v1/datasets` or task-history count fails before the POST — an incomplete marker is never compared, so leftover `Ready` cannot look newer than an empty baseline. A transport error or a non-2xx refresh POST is a failure: write the status/body artifacts, then stop. Do not wait for a leftover `Ready` or an older task-history row. After a 2xx POST, poll until a *new* completion: a later task-history row, a new `last_refresh` (success), or a new `error_message` (failure does not advance `last_refresh`). A leftover `Ready` or `Error` from before the POST is not completion. Keep the same marker checks on every poll.

The mapped journeys are in `features/`. Drive them with:

```bash
.agents/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh drive \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --evidence "$EVIDENCE_DIR" \
  --label "$LABEL"
```

`$LABEL` is `trunk` or `branch`. The helper writes, then queries, for baseline → reorder → rename → restore → empty-JSON on CSV, JSONL, `file_format: json` (array), and `json_pointer`, federated and Arrow-accelerated. Federated reads see the file immediately. Accelerated reads see it only after refresh. Emptying the JSON sources (`[]` / `{"data":[]}` / blank JSONL) must clear the JSON accelerations; CSV stays populated. An unregistered JSON key that changes type after registration is not in this HTTP drive; prove it with `create_listing_table_allows_json_unregistered_field_type_change_on_refresh`. A flattened object that becomes null is not in this HTTP drive; prove it with `json_refresh_allows_a_flattened_object_that_becomes_null` and `create_listing_table_allows_json_flatten_null_parent_refresh`. A mid-scan CSV replacement is not in this HTTP drive; prove it with `replacing_the_object_between_header_and_data_reads_fails_the_scan`.

## Evidence

Proof lives under `$EVIDENCE_DIR` (default `/opt/cursor/artifacts/verify-file-connector/`). Cleanup must not delete it.

Each label writes:

- `spicepod.yaml` and the source files at each step
- `sql-<step>-<dataset>.json` — the exact `POST /v1/sql` body and response
- `datasets-<step>.json` — `GET /v1/datasets?status=true`
- `refresh-<step>-<dataset>.{status,body}` — refresh HTTP status and body
- `ready-<step>.txt` — `/v1/ready` status and body
- `spiced.log` — stdout/stderr, including `WARN` / refresh-error lines that name `'amount_cents'`
- `summary.txt` — one line per step with sums and dataset status

Standards:

- Drive the shipped binary over HTTP. A unit test is not evidence.
- A SQL transport failure, a non-2xx `/v1/sql` response, a body that is not JSON, or a row missing `amount_cents` / `user_id` fails `drive` / `compare`. Valid SQL `NULL` sums (`null,null`) stay accepted. Response artifacts are still written.
- A failed baseline completion marker (transport, non-2xx, or invalid `/v1/datasets` / task-history count) fails `drive` / `compare` before the refresh POST. An incomplete marker is never compared.
- A refresh transport failure or a non-2xx `POST /v1/datasets/<name>/acceleration/refresh` fails `drive` / `compare` before polling. Status and body artifacts are still written.
- A doctor `/v1/ready` transport failure is refused. HTTP 503 after an expected refresh Error is still HTTP and is accepted.
- Capture the action (file rewrite, refresh POST) and the resulting sums/status/log, not only the last query.
- Re-run from a clean work dir. The helper is deterministic: same files, same SQL, same endpoints.
- Compare `trunk` vs `branch` with the same helper so the numbers are from the same recipe.

Trunk vs branch in one shot (builds are the caller's job):

```bash
.agents/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh compare \
  --trunk-spiced /path/to/spiced-trunk \
  --branch-spiced /path/to/spiced-branch \
  --evidence /opt/cursor/artifacts/verify-file-connector
```

Each side runs in a subshell. An `EXIT` trap installed before launch always runs `cleanup` for that side, so a readiness, doctor, or refresh failure still stops the runtime. The original failure status is preserved. If verification succeeds and cleanup fails, the helper reports the cleanup failure and exits with that status.

## Cleanup

```bash
.agents/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh cleanup --work "$WORK_DIR"
```

Sends `TERM` (then `KILL` if needed) only to the pid in `$WORK_DIR/spiced.pid` when `$WORK_DIR/spiced.identity` still names that same process (pid, executable, `/proc/<pid>/stat` starttime). An unreaped zombie (state `Z`) is stopped: `kill -0` still succeeds, but `/proc/<pid>/exe` is gone, so cleanup treats `Z` as gone instead of an ownership change. An unreadable process state is not treated as terminated. Refuses to signal when ownership cannot be established. Removes the work dir's runtime scratch (pidfile, identity, `.spice/`). Leaves `$EVIDENCE_DIR` untouched.

## Helpers

`helpers/verify-file-connector.sh` is executable. Subcommands: `seed`, `launch`, `doctor`, `drive`, `cleanup`, `compare`. Pass `--help` for flags.

## Feature map

See `features/README.md`. Seeded journeys: CSV, JSONL, `file_format: json`, and `json_pointer` reorder / rename / restore. Both federated and Arrow-accelerated. An unregistered JSON key that changes type after registration is proven by `create_listing_table_allows_json_unregistered_field_type_change_on_refresh`. A flattened object that becomes null (`user` → `null` with registered `user.name`) is proven by `json_refresh_allows_a_flattened_object_that_becomes_null` and `create_listing_table_allows_json_flatten_null_parent_refresh`. A mid-scan CSV object replacement is proven by `replacing_the_object_between_header_and_data_reads_fails_the_scan`. An ETag pins with If-Match only (`pin_with_etag_does_not_send_version`); the cluster codec restores that ETag via `FileGeneration`.
