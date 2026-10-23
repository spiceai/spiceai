---
name: verify-spiced
description: Launch a local spiced, check readiness, drive SQL/HTTP, capture evidence, and clean up. Use when verifying a spiceai/spiceai change against a running runtime — secret stores, file formats, validation errors, or any dataset query path.
---

# verify-spiced

Project-local verification for a built `spiced`. Do not claim a fix or a regression without a run of this loop on both trunk and the branch, with the commit SHA recorded next to the output.

## Features

Read the matching file under `features/` before running a journey:

- `features/env-secret-precedence.md` — last-declared env store, `file_path` isolation, and default `.env` export
- `features/file-format-validation.md` — structured `file_format` typos are a configuration error; common text extensions and other unknown formats fall through; document/text formats honor `file_extension`
- `features/secret-value-validation-errors.md` — secret-derived param values are not echoed

## Loop

### 1. Launch

Build `spiced` once per SHA. Prefer a dedicated `CARGO_TARGET_DIR` so trunk and branch do not fight the same incremental cache.

```bash
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ \
  cargo build -p spiced
BIN="$PWD/target/debug/spiced"   # or $CARGO_TARGET_DIR/debug/spiced
```

Start `spiced` from a temp working directory that holds the spicepod and any `.env` files. Bind HTTP/Flight/metrics to ephemeral localhost ports. Capture stdout/stderr to a log file. Record `$PID`.

Never print a real secret. Use dummy markers only (`from_custom`, `CANARYPGPW7731`).

### 2. Doctor

Poll until healthy. Do not `sleep` as a readiness wait.

```bash
for i in $(seq 1 40); do
  curl -sf "http://127.0.0.1:$HTTP/health" && break
  sleep 0.5
done
curl -sf "http://127.0.0.1:$HTTP/v1/ready" || true
```

Fail the run if `/health` never answers. `/v1/ready` may stay unready when the spicepod is *supposed* to have a dataset in `Error`.

### 3. Drive

Exercise the user-visible path the feature names:

- SQL: `curl -s -XPOST "http://127.0.0.1:$HTTP/v1/sql" -d "SELECT …"`
- Dataset status: `curl -s "http://127.0.0.1:$HTTP/v1/datasets?status=true"`
- Auth probes (secret precedence): `curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: $k" "http://127.0.0.1:$HTTP/v1/datasets"`

### 4. Evidence

Keep the command, the SHA, the date, and the verbatim output (or the canary-only slice). Save under `/opt/cursor/artifacts/`. A claim of before/after needs the same command on trunk and on the branch.

Never paste a real secret. For secret-echo checks, `grep` the canary marker only.

### 5. Cleanup

Kill the recorded PID. Do not `pkill -f`. Leave the binary and artifacts in place so the next journey can reuse the build.

```bash
kill "$PID"
wait "$PID" 2>/dev/null || true
```
