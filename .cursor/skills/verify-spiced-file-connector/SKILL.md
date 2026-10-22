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
.cursor/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh seed --work "$WORK_DIR"
```

Start only this instance, with logs and a pidfile under `$WORK_DIR`:

```bash
.cursor/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh launch \
  --spiced target/debug/spiced \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --flight "$FLIGHT_ADDR" \
  --metrics "$METRICS_ADDR"
```

Exact process command the helper runs (cwd is `$WORK_DIR`, so `file:` paths resolve):

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
.cursor/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh doctor \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --spiced target/debug/spiced
```

Doctor requires all of:

- `$WORK_DIR/spiced.pid` names a live process whose `/proc/<pid>/exe` is the launched `spiced`
- `GET /v1/ready` is reachable (200 `ready` on a clean launch; after an expected Error the helper reports the 503 and still requires HTTP)
- `GET /v1/datasets?status=true` lists `csv_fed`, `csv_acc`, `json_fed`, `json_acc`

Do not drive an instance that doctor did not start.

## Drive

HTTP SQL is the harness. `POST /v1/sql` with `Content-Type: text/plain` and `Accept: application/json`. Aliased sums are the observable:

```sql
SELECT sum(amount_cents) AS amount_cents, sum(user_id) AS user_id FROM <dataset>
```

On-demand refresh: `POST /v1/datasets/<name>/acceleration/refresh` then poll `GET /v1/datasets?status=true` until that dataset leaves `Refreshing`.

The mapped journeys are in `features/`. Drive them with:

```bash
.cursor/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh drive \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --evidence "$EVIDENCE_DIR" \
  --label "$LABEL"
```

`$LABEL` is `trunk` or `branch`. The helper writes, then queries, for baseline → reorder → rename → restore on both CSV and JSON, federated and Arrow-accelerated. Federated reads see the file immediately. Accelerated reads see it only after refresh.

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
- Capture the action (file rewrite, refresh POST) and the resulting sums/status/log, not only the last query.
- Re-run from a clean work dir. The helper is deterministic: same files, same SQL, same endpoints.
- Compare `trunk` vs `branch` with the same helper so the numbers are from the same recipe.

Trunk vs branch in one shot (builds are the caller's job):

```bash
.cursor/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh compare \
  --trunk-spiced /path/to/spiced-trunk \
  --branch-spiced /path/to/spiced-branch \
  --evidence /opt/cursor/artifacts/verify-file-connector
```

## Cleanup

```bash
.cursor/skills/verify-spiced-file-connector/helpers/verify-file-connector.sh cleanup --work "$WORK_DIR"
```

Sends `TERM` (then `KILL` if needed) only to the pid in `$WORK_DIR/spiced.pid`. Removes the work dir's runtime scratch (pidfile, `.spice/`). Leaves `$EVIDENCE_DIR` untouched.

## Helpers

`helpers/verify-file-connector.sh` is executable. Subcommands: `seed`, `launch`, `doctor`, `drive`, `cleanup`, `compare`. Pass `--help` for flags.

## Feature map

See `features/README.md`. Seeded journeys: CSV reorder / rename / restore, JSON reorder / rename / restore. Both federated and Arrow-accelerated.
