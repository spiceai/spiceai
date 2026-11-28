#!/usr/bin/env bash
# Drive `spice search` against a mock POST /v1/search the way a script would.
# usage: verify-spice-search-cli.sh <spice-binary>
set -euo pipefail

SPICE="$(realpath "${1:?usage: $0 <spice-binary>}")"
OWN_WORK_DIR=0
if [[ -z "${WORK_DIR:-}" ]]; then
  WORK_DIR="$(mktemp -d /tmp/verify-spice-search-cli-XXXX)"
  OWN_WORK_DIR=1
elif [[ -e "$WORK_DIR" && -n "$(ls -A "$WORK_DIR" 2>/dev/null)" ]]; then
  echo "WORK_DIR $WORK_DIR is not empty; pass an empty or new directory" >&2
  exit 2
fi
mkdir -p "$WORK_DIR"
WORK_DIR="$(cd "$WORK_DIR" && pwd)"
HELPER_DIR="$(cd "$(dirname "$0")" && pwd)"
PID=""
MOCK_DIR=""
MOCK_URL=""

cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    wait "$PID" 2>/dev/null || true
  fi
  if [[ "$OWN_WORK_DIR" == 1 && "${KEEP:-0}" != 1 ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

start_mock() {
  local status="${1:-200}"
  local dir="$WORK_DIR/mock-$status-$$-$RANDOM"
  mkdir -p "$dir"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    wait "$PID" 2>/dev/null || true
    PID=""
  fi
  python3 "$HELPER_DIR/mock_search_server.py" "$dir" "$status" >"$dir/port.out" 2>"$dir/server.log" &
  PID=$!
  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    if [[ -s "$dir/port" ]]; then
      MOCK_URL="http://127.0.0.1:$(cat "$dir/port")"
      MOCK_DIR="$dir"
      return
    fi
    kill -0 "$PID" 2>/dev/null || {
      echo "mock search server exited"
      cat "$dir/server.log" >&2
      exit 1
    }
    sleep 0.05
  done
  echo "mock search server did not publish a port"
  cat "$dir/server.log" >&2
  exit 1
}

assert_last_body() {
  local expected_json
  expected_json="$(cat)"
  EXPECTED_JSON="$expected_json" python3 - "$MOCK_DIR" <<'PY'
import json, os, pathlib, sys
d = pathlib.Path(sys.argv[1])
bodies = sorted(d.glob("body-*.json"))
if not bodies:
    raise SystemExit("no request bodies recorded")
body = json.loads(bodies[-1].read_text())
expected = json.loads(os.environ["EXPECTED_JSON"])
for key, value in expected.items():
    if key == "__absent__":
        for missing in value:
            if missing in body:
                raise SystemExit(f"did not expect {missing} in {body}")
        continue
    if body.get(key) != value:
        raise SystemExit(f"{key}: expected {value!r}, got {body.get(key)!r} in {body}")
print("body ok")
PY
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pass() {
  echo "ok  $*"
}

echo "binary: $("$SPICE" --version 2>/dev/null | head -1)"
echo "work:   $WORK_DIR"

HELP="$("$SPICE" search --help 2>/dev/null || true)"
echo "$HELP" | grep -q QUERY || fail "search --help must mention QUERY"
echo "$HELP" | grep -q -- "--datasets" || fail "search --help must mention --datasets"
echo "$HELP" | grep -q -- "--additional-columns" || fail "search --help must mention --additional-columns"
echo "$HELP" | grep -q -- "--where" || fail "search --help must mention --where"
echo "$HELP" | grep -q -- "--keywords" || fail "search --help must mention --keywords"
echo "$HELP" | grep -q -- "--limit" || fail "search --help must mention --limit"
if echo "$HELP" | grep -q -- "--model"; then
  fail "search --help must not advertise --model"
fi
if echo "$HELP" | grep -q validate_cloud_region_usage; then
  fail "search --help leaked an internal identifier"
fi
if echo "$HELP" | grep -q "Within the REPL you can scope"; then
  fail "search --help promised REPL-only scoping"
fi
pass "search --help is user-facing and lists wired scoping flags"

start_mock 200
OUT="$WORK_DIR/dolphins.json"
ERR="$WORK_DIR/dolphins.err"
set +e
"$SPICE" search dolphins --endpoint "$MOCK_URL" -o json >"$OUT" 2>"$ERR"
STATUS=$?
set -e
[[ "$STATUS" -eq 0 ]] || fail "spice search dolphins -o json exited $STATUS: $(cat "$ERR")"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$OUT" || fail "stdout is not valid JSON: $(cat "$OUT")"
if grep -q $'\xe2\xa0' "$OUT"; then
  fail "spinner bytes leaked into stdout"
fi
if command -v jq >/dev/null 2>&1; then
  jq -e '.results' "$OUT" >/dev/null || fail "jq could not read .results from piped JSON"
  pass "spice search dolphins -o json | jq"
else
  echo "note: jq is not installed; skipped jq parse (Python json.load passed)"
fi
assert_last_body <<'JSON'
{"text":"dolphins"}
JSON
pass "positional query reaches POST /v1/search"

start_mock 200
set +e
"$SPICE" search help --endpoint "$MOCK_URL" -o json >"$WORK_DIR/help.json" 2>"$WORK_DIR/help.err"
STATUS=$?
set -e
[[ "$STATUS" -eq 0 ]] || fail "spice search help exited $STATUS: $(cat "$WORK_DIR/help.err")"
assert_last_body <<'JSON'
{"text":"help"}
JSON
pass "help is a query term"

start_mock 200
set +e
"$SPICE" search dolphins --endpoint "$MOCK_URL" -o json \
  --datasets docs,wiki \
  --additional-columns title,url \
  --where "status = 'open'" \
  --keywords mammal,cetacean \
  --limit 5 \
  >"$WORK_DIR/scoped.json" 2>"$WORK_DIR/scoped.err"
STATUS=$?
set -e
[[ "$STATUS" -eq 0 ]] || fail "scoped search exited $STATUS: $(cat "$WORK_DIR/scoped.err")"
assert_last_body <<'JSON'
{"text":"dolphins","datasets":["docs","wiki"],"additional_columns":["title","url"],"where":"status = 'open'","keywords":["mammal","cetacean"],"limit":5,"__absent__":["model"]}
JSON
pass "each scoping flag is in the request body"

set +e
"$SPICE" search -o json < /dev/null >"$WORK_DIR/empty.out" 2>"$WORK_DIR/empty.err"
STATUS=$?
set -e
[[ "$STATUS" -eq 2 ]] || fail "empty query exited $STATUS, expected 2: $(cat "$WORK_DIR/empty.err")"
[[ ! -s "$WORK_DIR/empty.out" ]] || fail "empty query wrote stdout: $(cat "$WORK_DIR/empty.out")"
grep -q "A search query is required" "$WORK_DIR/empty.err" || fail "empty query stderr: $(cat "$WORK_DIR/empty.err")"
pass "empty query exits 2 on stderr"

start_mock 500
set +e
"$SPICE" search dolphins --endpoint "$MOCK_URL" -o json >"$WORK_DIR/http500.out" 2>"$WORK_DIR/http500.err"
STATUS=$?
set -e
[[ "$STATUS" -eq 1 ]] || fail "HTTP 500 exited $STATUS, expected 1: $(cat "$WORK_DIR/http500.err")"
[[ ! -s "$WORK_DIR/http500.out" ]] || fail "HTTP 500 wrote stdout: $(cat "$WORK_DIR/http500.out")"
grep -q 500 "$WORK_DIR/http500.err" || fail "HTTP 500 stderr: $(cat "$WORK_DIR/http500.err")"
pass "HTTP error exits 1 on stderr"

set +e
"$SPICE" search dolphins --endpoint "http://127.0.0.1:1" -o json >"$WORK_DIR/conn.out" 2>"$WORK_DIR/conn.err"
STATUS=$?
set -e
[[ "$STATUS" -eq 1 ]] || fail "connection failure exited $STATUS, expected 1: $(cat "$WORK_DIR/conn.err")"
[[ ! -s "$WORK_DIR/conn.out" ]] || fail "connection failure wrote stdout: $(cat "$WORK_DIR/conn.out")"
grep -qiE "Failed to connect|unavailable" "$WORK_DIR/conn.err" || fail "connection failure stderr: $(cat "$WORK_DIR/conn.err")"
pass "connection failure exits 1 on stderr"

start_mock 200
set +e
echo dolphins | "$SPICE" search --endpoint "$MOCK_URL" -o json >"$WORK_DIR/pipe.json" 2>"$WORK_DIR/pipe.err"
SPICE_STATUS=$?
set -e
[[ "$SPICE_STATUS" -eq 0 ]] || fail "piped stdin search exited $SPICE_STATUS: $(cat "$WORK_DIR/pipe.err")"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$WORK_DIR/pipe.json" \
  || fail "piped stdout is not valid JSON: $(cat "$WORK_DIR/pipe.json")"
if command -v jq >/dev/null 2>&1; then
  jq -e '.results' <"$WORK_DIR/pipe.json" >/dev/null || fail "jq failed on piped JSON: $(cat "$WORK_DIR/pipe.json")"
  pass "echo dolphins | spice search -o json | jq"
else
  echo "note: jq is not installed; skipped jq parse (Python json.load passed)"
fi

echo "all cases passed"
