---
name: verify-ready-allow-unhealthy
description: Drive a real `spiced` over HTTP to prove `/v1/ready?allow_unhealthy=true` stays ready while a never-loaded dataset retries after its first failure, while plain `/v1/ready` stays 503 and a first-attempt Refreshing dataset still blocks. Use when changing `RuntimeStatus::is_ready_allowing_unhealthy` or the `/v1/ready` handler.
---

# Verify `/v1/ready?allow_unhealthy=true` retry stickiness

Unit tests in `crates/runtime-status` and `crates/runtime/src/http/v1/ready.rs` cover the status tracker. This skill proves the probe a Kubernetes readiness check calls, on a built `spiced`, with a healthy local dataset plus a dataset whose first load fails and retries.

Do not treat unit tests or a compile as proof. A claim of before/after needs the same helper on trunk and on the branch, with the commit SHA recorded next to the output.

## Build

```bash
CC=cc CXX=g++ cargo build -p spiced --no-default-features
```

The file and HTTP connectors and the Cayenne accelerator are always linked. Extra connectors are not needed.

## Loop

### 1. Launch

```bash
WORK_DIR="${WORK_DIR:-/tmp/spice-ready-allow-unhealthy}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18490}"
FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15451}"
METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19490}"

.agents/skills/verify-ready-allow-unhealthy/helpers/verify-ready-allow-unhealthy.sh seed --work "$WORK_DIR"

.agents/skills/verify-ready-allow-unhealthy/helpers/verify-ready-allow-unhealthy.sh launch \
  --spiced target/debug/spiced \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --flight "$FLIGHT_ADDR" \
  --metrics "$METRICS_ADDR"
```

The seeded spicepod has:

- `nation` — local CSV, should reach `Ready`
- `missing` — HTTP CSV whose first GET succeeds (schema) and later GETs hang then return 500, Cayenne-accelerated, so the first refresh fails and retries (`Error` ↔ `Refreshing`) without a remote bucket

Bind HTTP/Flight/metrics to ephemeral localhost ports when running two binaries at once. Capture stdout/stderr to `$WORK_DIR/spiced.log`. Record the pid.

Never `pkill -f`. Ready is not an open port: launch succeeds when `GET /health` answers. Plain `/v1/ready` is expected to stay `503` for this spicepod.

### 2. Doctor

```bash
.agents/skills/verify-ready-allow-unhealthy/helpers/verify-ready-allow-unhealthy.sh doctor \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --spiced target/debug/spiced
```

Doctor requires the pid/identity still match, `/health` reachable, and `GET /v1/datasets?status=true` listing `nation` and `missing`. `/v1/ready` may be 503.

### 3. Drive

Poll once a second:

- `GET /v1/ready`
- `GET /v1/ready?allow_unhealthy=true`
- `GET /v1/datasets?status=true` (dataset `status` values)

```bash
.agents/skills/verify-ready-allow-unhealthy/helpers/verify-ready-allow-unhealthy.sh poll \
  --work "$WORK_DIR" \
  --http "$HTTP_ADDR" \
  --evidence "$EVIDENCE_DIR" \
  --label "$LABEL" \
  --seconds 330
```

`$LABEL` is `trunk` or `branch`. Default `--seconds` is 60; a flip-count claim needs 5+ minutes (330s) on the same spicepod before and after the change.

### 4. Evidence

Proof lives under `$EVIDENCE_DIR` (default `/opt/cursor/artifacts/verify-ready-allow-unhealthy/`). Cleanup must not delete it.

Each label writes:

- `spicepod.yaml` and `data/nation.csv`
- `poll.csv` — one row per second: unix time, plain code, allow_unhealthy code, nation status, missing status
- `summary.txt` — SHA, binary version, duration, flip counts, status timeline
- `spiced.log`

Expected after the sticky-failure fix:

| probe | expected |
|---|---|
| `GET /v1/ready` | 503 every sample |
| `GET /v1/ready?allow_unhealthy=true` | 503 while `missing` is still on its first attempt (`Refreshing` before any `Error`); 200 from the first `Error` onward, including later `Refreshing` retries (0 flips after that) |
| `missing` dataset status | still `Error` / `Refreshing` as today |

Before the fix, `allow_unhealthy` is 503 on first-attempt `Refreshing`, 200 on `Error`, then 503 again on each retry `Refreshing`. A first-attempt `Refreshing` dataset (no failure yet) must still keep `allow_unhealthy` at 503.

Trunk vs branch:

```bash
.agents/skills/verify-ready-allow-unhealthy/helpers/verify-ready-allow-unhealthy.sh compare \
  --trunk-spiced /path/to/spiced-trunk \
  --branch-spiced /path/to/spiced-branch \
  --evidence /opt/cursor/artifacts/verify-ready-allow-unhealthy \
  --seconds 330
```

Each side runs in a subshell with its own work dir and ports. An `EXIT` trap installed before launch always runs `cleanup` for that side.

### 5. Cleanup

```bash
.agents/skills/verify-ready-allow-unhealthy/helpers/verify-ready-allow-unhealthy.sh cleanup --work "$WORK_DIR"
```

Sends `TERM` (then `KILL` if needed) only to the pid in `$WORK_DIR/spiced.pid`. Leaves `$EVIDENCE_DIR` untouched.

## Helpers

`helpers/verify-ready-allow-unhealthy.sh` is executable. Subcommands: `seed`, `launch`, `doctor`, `poll`, `cleanup`, `compare`. Pass `--help` for flags.
