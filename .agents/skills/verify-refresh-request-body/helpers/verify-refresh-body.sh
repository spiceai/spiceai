#!/usr/bin/env bash
# Drive POST /v1/datasets/{name}/acceleration/refresh with each body shape on a real spiced.
# usage: verify-refresh-body.sh <spiced-binary> | --url http://host:port
set -euo pipefail

DATASET="${DATASET:-events}"
URL=""
PID=""
if [[ "${1:-}" == "--url" ]]; then
  URL="${2:?--url needs a value}"
else
  SPICED="$(realpath "${1:?usage: $0 <spiced-binary> | --url http://host:port}")"
  HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18391}"
  FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15391}"
  METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19391}"
  # Only a work directory this script created is removed; a WORK_DIR you pass is yours.
  OWN_WORK_DIR=0
  if [[ -z "${WORK_DIR:-}" ]]; then
    WORK_DIR="$(mktemp -d /tmp/verify-refresh-body-XXXX)"
    OWN_WORK_DIR=1
  fi
  mkdir -p "$WORK_DIR/data"
  # Absolute, so the CSV path in the Spicepod and the log redirect work after `cd`.
  WORK_DIR="$(cd "$WORK_DIR" && pwd)"
  printf 'id,name\n1,a\n2,b\n' > "$WORK_DIR/data/events.csv"
  cat > "$WORK_DIR/spicepod.yaml" <<YAML
version: v1
kind: Spicepod
name: verify-refresh-body
datasets:
  - from: file:$WORK_DIR/data/events.csv
    name: $DATASET
    params: {file_format: csv}
    acceleration: {enabled: true, engine: arrow, refresh_mode: full}
YAML
  cleanup() {
    if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
      kill "$PID"; wait "$PID" 2>/dev/null || true
    fi
    if [[ "$OWN_WORK_DIR" == 1 && "${KEEP:-0}" != 1 ]]; then rm -rf "$WORK_DIR"; fi
  }
  trap cleanup EXIT
  (cd "$WORK_DIR" && exec "$SPICED" --telemetry-enabled=false \
    --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" . \
    > "$WORK_DIR/spiced.log" 2>&1) &
  PID=$!
  URL="http://$HTTP_ADDR"
  echo "binary: $("$SPICED" --version 2>/dev/null | head -1)"
  ready=0
  for _ in $(seq 90); do
    if [[ "$(curl -s "$URL/v1/ready" || true)" == "ready" ]]; then ready=1; break; fi
    kill -0 "$PID" 2>/dev/null || { echo "spiced exited"; tail -n 20 "$WORK_DIR/spiced.log"; exit 1; }
    sleep 1
  done
  [[ $ready == 1 ]] || { echo "not ready after 90s"; tail -n 20 "$WORK_DIR/spiced.log"; exit 1; }
fi

EP="$URL/v1/datasets/$DATASET/acceleration/refresh"
run() {
  local label="$1"; shift
  local out
  out="$(curl -sS -X POST "$EP" "$@" -w $'\n%{http_code}')"
  printf '%-58s | HTTP %s | %s\n' "$label" "${out##*$'\n'}" "${out%$'\n'*}"
}

run "no body, no Content-Type"
run "empty body, Content-Type: application/json" -H 'Content-Type: application/json' --data-binary ''
run "empty body, Content-Type: x-www-form-urlencoded" -H 'Content-Type: application/x-www-form-urlencoded' --data-binary ''
run "whitespace body, Content-Type: application/json" -H 'Content-Type: application/json' --data-binary $' \n'
run "{} with Content-Type: application/json" -H 'Content-Type: application/json' -d '{}'
run "{\"refresh_mode\":\"full\"} with application/json" -H 'Content-Type: application/json' -d '{"refresh_mode":"full"}'
run "{\"refresh_mode\":\"bogus\"} via curl -d (form Content-Type)" -d '{"refresh_mode":"bogus"}'
run "{\"refresh_mode\":\"bogus\"} with no Content-Type" -H 'Content-Type:' --data-binary '{"refresh_mode":"bogus"}'
run "malformed {not json, application/json" -H 'Content-Type: application/json' -d '{not json'
run "{\"refresh_mode\":\"bogus\"} with application/json" -H 'Content-Type: application/json' -d '{"refresh_mode":"bogus"}'
