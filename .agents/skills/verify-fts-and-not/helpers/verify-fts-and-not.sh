#!/usr/bin/env bash
# Drive text_search and POST /v1/search AND NOT queries on a real spiced.
# usage: verify-fts-and-not.sh <spiced-binary> | --url http://host:port
set -euo pipefail

json_count() {
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d, list) else d)'
}

json_ids() {
  python3 -c 'import json,sys; rows=json.load(sys.stdin); print(",".join(str(r["id"]) for r in rows))'
}

json_n() {
  python3 -c 'import json,sys; rows=json.load(sys.stdin); print(rows[0]["n"] if rows else "missing")'
}

json_search_len() {
  python3 -c '
import json,sys
raw=sys.stdin.read().strip()
if not raw:
    print("empty")
    raise SystemExit(0)
try:
    d=json.loads(raw)
except json.JSONDecodeError:
    print("non-json")
    raise SystemExit(0)
if isinstance(d, dict) and "results" in d:
    print(len(d["results"]))
else:
    print("non-json")
'
}

sql() {
  local q="$1"
  curl -sS --max-time 30 -X POST "$URL/v1/sql" \
    -H 'Content-Type: application/json' -H 'Accept: application/json' \
    --data-binary "$(python3 -c 'import json,sys; print(json.dumps({"sql": sys.argv[1]}))' "$q")"
}

search() {
  local text="$1"
  local body
  body="$(python3 -c 'import json,sys; print(json.dumps({"text": sys.argv[1], "datasets": ["docs"], "limit": 100, "additional_columns": ["id"]}))' "$text")"
  curl -sS --max-time 30 -X POST "$URL/v1/search" \
    -H 'Content-Type: application/json' -H 'Accept: application/json' \
    -w '\n%{http_code}' \
    --data-binary "$body"
}

URL=""
PID=""
if [[ "${1:-}" == "--url" ]]; then
  URL="${2:?--url needs a value}"
else
  SPICED="$(realpath "${1:?usage: $0 <spiced-binary> | --url http://host:port}")"
  HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18493}"
  FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15493}"
  METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19493}"
  OWN_WORK_DIR=0
  if [[ -z "${WORK_DIR:-}" ]]; then
    WORK_DIR="$(mktemp -d /tmp/verify-fts-and-not-XXXX)"
    OWN_WORK_DIR=1
  elif [[ -e "$WORK_DIR" && -n "$(ls -A "$WORK_DIR" 2>/dev/null)" ]]; then
    echo "WORK_DIR $WORK_DIR is not empty; pass an empty or new directory"; exit 2
  fi
  mkdir -p "$WORK_DIR"
  WORK_DIR="$(cd "$WORK_DIR" && pwd)"
  cat > "$WORK_DIR/docs.csv" <<'CSV'
id,title,comment
1,spice runtime,dolphins play
2,spice draft,dolphins furiously
3,runtime notes,pearls and dolphins
4,draft only,pearls furiously
5,other,only pearls
6,spice,furiously alone
CSV
  cat > "$WORK_DIR/spicepod.yaml" <<'YAML'
version: v1
kind: Spicepod
name: verify-fts-and-not
datasets:
  - from: file:docs.csv
    name: docs
    params:
      file_format: csv
    acceleration:
      enabled: true
      engine: cayenne
      mode: memory
      refresh_mode: full
    columns:
      - name: title
        full_text_search:
          enabled: true
          row_id: [id]
      - name: comment
        full_text_search:
          enabled: true
          row_id: [id]
YAML
  cleanup() {
    if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
      kill "$PID"; wait "$PID" 2>/dev/null || true
    fi
    if [[ "$OWN_WORK_DIR" == 1 && "${KEEP:-0}" != 1 ]]; then rm -rf "$WORK_DIR"; fi
  }
  trap cleanup EXIT
  if curl -s -o /dev/null --max-time 2 "http://$HTTP_ADDR/v1/ready"; then
    echo "something is already listening on $HTTP_ADDR; stop it or set HTTP_ADDR"; exit 2
  fi
  (cd "$WORK_DIR" && exec "$SPICED" --telemetry-enabled=false \
    --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" . \
    > "$WORK_DIR/spiced.log" 2>&1) &
  PID=$!
  URL="http://$HTTP_ADDR"
  echo "binary: $("$SPICED" --version 2>/dev/null | head -1)"
  echo "binary_path: $SPICED"
  echo "binary_sha256: $(sha256sum "$SPICED" | awk '{print $1}')"
  echo "git_head: $(git rev-parse HEAD 2>/dev/null || true)"
  ready=0
  deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if [[ "$(curl -s --max-time 2 "$URL/v1/ready" || true)" == "ready" ]]; then ready=1; break; fi
    kill -0 "$PID" 2>/dev/null || { echo "spiced exited"; tail -n 40 "$WORK_DIR/spiced.log"; exit 1; }
    sleep 1
  done
  [[ $ready == 1 ]] || { echo "not ready after 90s"; tail -n 40 "$WORK_DIR/spiced.log"; exit 1; }
fi

# FTS is built during dataset load; poll until the known-positive query hits.
indexed=0
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  body="$(sql "SELECT COUNT(*) AS n FROM text_search(docs, 'dolphins', comment)" || true)"
  if n="$(printf '%s' "$body" | json_n 2>/dev/null)" && [[ "$n" == "3" ]]; then
    indexed=1
    break
  fi
  sleep 1
done
if [[ $indexed != 1 ]]; then
  echo "text_search(docs, 'dolphins', comment) did not return 3 rows"
  echo "$body"
  exit 1
fi

LAST_N=""
sql_case() {
  local label="$1"
  local query="$2"
  local column="$3"
  local count_sql="SELECT COUNT(*) AS n FROM text_search(docs, '$query', $column)"
  local ids_sql="SELECT id FROM text_search(docs, '$query', $column) ORDER BY id"
  local n ids
  n="$(sql "$count_sql" | json_n)"
  ids="$(sql "$ids_sql" | json_ids)"
  printf 'sql | %s | count=%s | ids=%s\n' "$label" "$n" "$ids"
  LAST_N="$n"
}

search_case() {
  local label="$1"
  local query="$2"
  local out http body n
  out="$(search "$query")"
  http="${out##*$'\n'}"
  body="${out%$'\n'*}"
  if [[ "$http" != "200" ]]; then
    printf 'search | %s | skipped HTTP %s (POST /v1/search is registered only with the models feature)\n' "$label" "$http"
    LAST_N="skipped"
    return
  fi
  n="$(printf '%s' "$body" | json_search_len)"
  printf 'search | %s | count=%s\n' "$label" "$n"
  LAST_N="$n"
}

compare() {
  local label="$1"
  local left="$2"
  local right="$3"
  if [[ "$left" == "$right" && "$left" != "0" ]]; then
    printf 'compare | %s | MATCH count=%s\n' "$label" "$left"
  else
    printf 'compare | %s | MISMATCH and_not=%s minus=%s\n' "$label" "$left" "$right"
  fi
}

sql_case 'dolphins AND NOT furiously' 'dolphins AND NOT furiously' comment
and_not="$LAST_N"
sql_case 'dolphins -furiously' 'dolphins -furiously' comment
minus="$LAST_N"
sql_case 'dolphins NOT furiously' 'dolphins NOT furiously' comment
sql_case 'dolphins AND -furiously' 'dolphins AND -furiously' comment
sql_case 'dolphins AND (NOT furiously)' 'dolphins AND (NOT furiously)' comment
paren="$LAST_N"
sql_case 'comment:(dolphins OR pearls) AND NOT furiously' 'comment:(dolphins OR pearls) AND NOT furiously' comment
grouped_and_not="$LAST_N"
sql_case 'comment:(dolphins OR pearls) -furiously' 'comment:(dolphins OR pearls) -furiously' comment
grouped_minus="$LAST_N"
sql_case 'title:(spice OR runtime) AND NOT draft' 'title:(spice OR runtime) AND NOT draft' title
doc_and_not="$LAST_N"
sql_case 'title:(spice OR runtime) -draft' 'title:(spice OR runtime) -draft' title
doc_minus="$LAST_N"
sql_case 'NOT furiously' 'NOT furiously' comment
leading_not="$LAST_N"
sql_case '-furiously' '-furiously' comment
leading_minus="$LAST_N"

search_case 'dolphins AND NOT furiously' 'dolphins AND NOT furiously'
search_and_not="$LAST_N"
search_case 'dolphins -furiously' 'dolphins -furiously'
search_minus="$LAST_N"
search_case 'title:(spice OR runtime) AND NOT draft' 'title:(spice OR runtime) AND NOT draft'
search_doc_and_not="$LAST_N"
search_case 'title:(spice OR runtime) -draft' 'title:(spice OR runtime) -draft'
search_doc_minus="$LAST_N"

compare 'sql dolphins AND NOT vs -' "$and_not" "$minus"
compare 'sql dolphins AND (NOT) vs -' "$paren" "$minus"
compare 'sql grouped OR AND NOT vs -' "$grouped_and_not" "$grouped_minus"
compare 'sql documented title AND NOT vs -' "$doc_and_not" "$doc_minus"
compare 'sql leading NOT vs -' "$leading_not" "$leading_minus"
if [[ "$search_and_not" != "skipped" ]]; then
  compare 'search dolphins AND NOT vs -' "$search_and_not" "$search_minus"
  compare 'search documented title AND NOT vs -' "$search_doc_and_not" "$search_doc_minus"
fi
