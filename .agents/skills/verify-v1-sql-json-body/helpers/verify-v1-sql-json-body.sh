#!/usr/bin/env bash
# Drive POST /v1/sql with each JSON and text body shape on a real spiced.
# usage: verify-v1-sql-json-body.sh <spiced-binary> | --url http://host:port
set -euo pipefail

URL=""
PID=""
if [[ "${1:-}" == "--url" ]]; then
  URL="${2:?--url needs a value}"
else
  SPICED="$(realpath "${1:?usage: $0 <spiced-binary> | --url http://host:port}")"
  HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18392}"
  FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15392}"
  METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19392}"
  # Only a work directory this script created is removed; a WORK_DIR you pass is yours.
  OWN_WORK_DIR=0
  if [[ -z "${WORK_DIR:-}" ]]; then
    WORK_DIR="$(mktemp -d /tmp/verify-v1-sql-json-body-XXXX)"
    OWN_WORK_DIR=1
  elif [[ -e "$WORK_DIR" && -n "$(ls -A "$WORK_DIR" 2>/dev/null)" ]]; then
    echo "WORK_DIR $WORK_DIR is not empty; pass an empty or new directory"; exit 2
  fi
  mkdir -p "$WORK_DIR"
  WORK_DIR="$(cd "$WORK_DIR" && pwd)"
  cat > "$WORK_DIR/spicepod.yaml" <<'YAML'
version: v1
kind: Spicepod
name: verify-v1-sql-json-body
YAML
  cleanup() {
    if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
      kill "$PID"; wait "$PID" 2>/dev/null || true
    fi
    if [[ "$OWN_WORK_DIR" == 1 && "${KEEP:-0}" != 1 ]]; then rm -rf "$WORK_DIR"; fi
  }
  trap cleanup EXIT
  # Refuse to start if something already answers on HTTP_ADDR: the results would come
  # from that process, not from the binary under test.
  if curl -s -o /dev/null --max-time 2 "http://$HTTP_ADDR/v1/ready"; then
    echo "something is already listening on $HTTP_ADDR; stop it or set HTTP_ADDR"; exit 2
  fi
  (cd "$WORK_DIR" && exec "$SPICED" --telemetry-enabled=false \
    --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" . \
    > "$WORK_DIR/spiced.log" 2>&1) &
  PID=$!
  URL="http://$HTTP_ADDR"
  echo "binary: $("$SPICED" --version 2>/dev/null | head -1)"
  echo "binary_sha256: $(sha256sum "$SPICED" | awk '{print $1}')"
  echo "git_head: $(git rev-parse HEAD 2>/dev/null || true)"
  ready=0
  deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if [[ "$(curl -s --max-time 2 "$URL/v1/ready" || true)" == "ready" ]]; then ready=1; break; fi
    kill -0 "$PID" 2>/dev/null || { echo "spiced exited"; tail -n 20 "$WORK_DIR/spiced.log"; exit 1; }
    sleep 1
  done
  [[ $ready == 1 ]] || { echo "not ready after 90s"; tail -n 20 "$WORK_DIR/spiced.log"; exit 1; }
fi

EP="$URL/v1/sql"
run() {
  local label="$1"; shift
  local out
  out="$(curl -sS --max-time 30 -X POST "$EP" "$@" -w $'\n%{http_code}')"
  printf '%-72s | HTTP %s | %s\n' "$label" "${out##*$'\n'}" "${out%$'\n'*}"
}

run '{"sql":"select 1 as one"} application/json' \
  -H 'Content-Type: application/json' -H 'Accept: application/json' \
  --data-binary '{"sql":"select 1 as one"}'
run '{"sql":"select 1 as one","parameters":null} application/json' \
  -H 'Content-Type: application/json' -H 'Accept: application/json' \
  --data-binary '{"sql":"select 1 as one","parameters":null}'
run '{"sql":"select 1 as one","parameters":[]} application/json; charset=utf-8' \
  -H 'Content-Type: application/json; charset=utf-8' -H 'Accept: application/json' \
  --data-binary '{"sql":"select 1 as one","parameters":[]}'
run '{"sql":"select 1 as one"} Application/JSON' \
  -H 'Content-Type: Application/JSON' -H 'Accept: application/json' \
  --data-binary '{"sql":"select 1 as one"}'
run '{"sql":"select $1 as one","parameters":[1]} application/json' \
  -H 'Content-Type: application/json' -H 'Accept: application/json' \
  --data-binary '{"sql":"select $1 as one","parameters":[1]}'
run '{"sql":"select :foo + 1 as the_answer","parameters":{"foo":41}} application/json' \
  -H 'Content-Type: application/json' -H 'Accept: application/json' \
  --data-binary '{"sql":"select :foo + 1 as the_answer","parameters":{"foo":41}}'
run 'select 1 as one text/plain' \
  -H 'Content-Type: text/plain' -H 'Accept: application/json' \
  --data-binary 'select 1 as one'
