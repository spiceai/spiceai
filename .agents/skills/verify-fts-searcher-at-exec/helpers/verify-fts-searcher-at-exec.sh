#!/usr/bin/env bash
# Prove text_search and POST /v1/search track a Cayenne full refresh.
# usage: verify-fts-searcher-at-exec.sh <spiced-binary>
set -euo pipefail

SPICED="$(realpath "${1:?usage: $0 <spiced-binary>}")"
INDEX_STORE="${INDEX_STORE:-both}"
HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18424}"
FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15424}"
METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19424}"

run_one() {
  local store="$1"
  local PID=""
  local OWN_WORK_DIR=0
  local WORK_DIR="${WORK_DIR:-}"
  if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$(mktemp -d /tmp/verify-fts-searcher-at-exec-XXXX)"
    OWN_WORK_DIR=1
  elif [[ -e "$WORK_DIR" && -n "$(ls -A "$WORK_DIR" 2>/dev/null)" ]]; then
    echo "WORK_DIR $WORK_DIR is not empty; pass an empty or new directory" >&2
    return 2
  fi
  mkdir -p "$WORK_DIR"
  WORK_DIR="$(cd "$WORK_DIR" && pwd)"
  mkdir -p "$WORK_DIR/cayenne-data" "$WORK_DIR/cayenne-metadata"
  printf 'id,text\n1,apple\n2,banana\n3,cherry\n4,durian\n' > "$WORK_DIR/docs.csv"

  local index_yaml
  if [[ "$store" == "file" ]]; then
    mkdir -p "$WORK_DIR/fts"
    index_yaml=$'          index_store: file\n          index_directory: fts'
  else
    index_yaml='          index_store: memory'
  fi

  cat > "$WORK_DIR/spicepod.yaml" <<YAML
version: v1
kind: Spicepod
name: verify-fts-searcher-at-exec
runtime:
  caching:
    sql_results:
      enabled: false
    search_results:
      enabled: false
datasets:
  - from: file:docs.csv
    name: docs
    params:
      file_format: csv
    columns:
      - name: text
        full_text_search:
          enabled: true
          row_id: id
${index_yaml}
    acceleration:
      enabled: true
      engine: cayenne
      mode: file
      refresh_mode: full
      primary_key: id
      params:
        cayenne_file_path: cayenne-data
        cayenne_metadata_dir: cayenne-metadata
YAML

  cleanup() {
    if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
      kill "$PID"
      wait "$PID" 2>/dev/null || true
    fi
    if [[ "$OWN_WORK_DIR" == 1 && "${KEEP:-0}" != 1 ]]; then
      rm -rf "$WORK_DIR"
    fi
  }
  trap cleanup RETURN

  if curl -s -o /dev/null --max-time 2 "http://$HTTP_ADDR/v1/ready"; then
    echo "something is already listening on $HTTP_ADDR; stop it or set HTTP_ADDR" >&2
    return 2
  fi

  (cd "$WORK_DIR" && exec "$SPICED" --telemetry-enabled=false \
    --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" . \
    > "$WORK_DIR/spiced.log" 2>&1) &
  PID=$!
  local URL="http://$HTTP_ADDR"
  echo "index_store=$store"
  echo "binary: $("$SPICED" --version 2>/dev/null | head -1)"
  echo "sha_note: pass GIT_SHA to record the commit this binary was built from"

  local ready=0
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if [[ "$(curl -s --max-time 2 "$URL/v1/ready" || true)" == "ready" ]]; then
      ready=1
      break
    fi
    kill -0 "$PID" 2>/dev/null || {
      echo "spiced exited"
      tail -n 40 "$WORK_DIR/spiced.log"
      return 1
    }
    sleep 1
  done
  if [[ $ready != 1 ]]; then
    echo "not ready after 120s"
    tail -n 40 "$WORK_DIR/spiced.log"
    return 1
  fi

  sql() {
    curl -sS --max-time 30 -X POST "$URL/v1/sql" \
      -H 'Content-Type: text/plain' \
      --data-binary "$1"
  }

  search_api() {
    curl -sS --max-time 30 -X POST "$URL/v1/search" \
      -H 'Content-Type: application/json' \
      -d "{\"text\":\"$1\",\"datasets\":[\"docs\"]}"
  }

  echo "--- before refresh ---"
  echo "sql banana: $(sql "SELECT id FROM text_search(docs, 'banana', text) ORDER BY id")"
  echo "search banana: $(search_api banana)"

  printf 'id,text\n1,apple\n3,walrus\n4,durian\n5,zephyrine\n' > "$WORK_DIR/docs.csv"
  echo "refresh: $(curl -sS --max-time 30 -w ' HTTP %{http_code}' -X POST "$URL/v1/datasets/docs/acceleration/refresh")"
  echo

  deadline=$((SECONDS + 60))
  local refreshed=0
  while (( SECONDS < deadline )); do
    if sql "SELECT text FROM docs WHERE id = 3" | grep -q walrus; then
      refreshed=1
      break
    fi
    sleep 1
  done
  if [[ $refreshed != 1 ]]; then
    echo "docs.id=3 did not become walrus after refresh"
    tail -n 40 "$WORK_DIR/spiced.log"
    return 1
  fi

  echo "--- after refresh ---"
  echo "sql banana (same): $(sql "SELECT id FROM text_search(docs, 'banana', text) ORDER BY id")"
  echo "sql banana (reworded): $(sql "SELECT id FROM text_search(docs, 'banana', text) WHERE true ORDER BY id")"
  echo "sql walrus: $(sql "SELECT id FROM text_search(docs, 'walrus', text) ORDER BY id")"
  echo "sql zephyrine: $(sql "SELECT id FROM text_search(docs, 'zephyrine', text) ORDER BY id")"
  echo "search banana: $(search_api banana)"
  echo "search walrus: $(search_api walrus)"
  echo "search zephyrine: $(search_api zephyrine)"
}

echo "GIT_SHA=${GIT_SHA:-unspecified} date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
case "$INDEX_STORE" in
  memory|file) run_one "$INDEX_STORE" ;;
  both)
    run_one memory
    echo
    run_one file
    ;;
  *)
    echo "INDEX_STORE must be memory, file, or both" >&2
    exit 2
    ;;
esac
