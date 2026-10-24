#!/usr/bin/env bash
# Verify DuckDB (and SQLite) decimal / against a running spiced.
set -euo pipefail

skill_dir=$(cd "$(dirname "$0")" && pwd)
http=${HTTP:-127.0.0.1:18490}
flight=${FLIGHT:-127.0.0.1:18491}
if [[ -n "${SPICED:-}" ]]; then
  spiced=$SPICED
elif command -v spiced >/dev/null 2>&1; then
  spiced=$(command -v spiced)
elif [[ -x "${PWD}/target/debug/spiced" ]]; then
  spiced=${PWD}/target/debug/spiced
else
  echo "set SPICED to a spiced binary" >&2
  exit 1
fi

workdir=${WORKDIR:-$(mktemp -d /tmp/verify-duckdb-decimal-division.XXXXXX)}
mkdir -p "$workdir"
parquet=$workdir/tricky.parquet
pod=$workdir/spicepod.yaml

if ! python3 -c 'import pyarrow, pyarrow.parquet' >/dev/null 2>&1; then
  python3 -m pip install --user pyarrow
fi
python3 "$skill_dir/gen.py" "$parquet"

cat >"$pod" <<EOF
version: v1
kind: Spicepod
name: verify-duckdb-decimal-division
datasets:
  - from: file://${parquet}
    name: t_fed
    params:
      file_format: parquet
  - from: file://${parquet}
    name: t_duckdb
    params:
      file_format: parquet
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: full
      params:
        duckdb_file: ${workdir}/t_duckdb.db
  - from: file://${parquet}
    name: t_sqlite
    params:
      file_format: parquet
    acceleration:
      enabled: true
      engine: sqlite
      mode: memory
      refresh_mode: full
EOF

echo "SPICED=$spiced"
echo "WORKDIR=$workdir"
"$spiced" --version || true

# Only this invocation's child. Empty when REUSE=1 (we did not launch).
started_pid=""
cleanup() {
  if [[ "${KEEP:-}" == 1 ]]; then
    return 0
  fi
  if [[ -n "${started_pid}" ]]; then
    kill "$started_pid" 2>/dev/null || true
    wait "$started_pid" 2>/dev/null || true
  fi
}

ready_url="http://${http}/v1/ready"
if [[ "${REUSE:-}" == 1 ]] && curl -sf "$ready_url" >/dev/null 2>&1; then
  echo "reusing the spiced already listening on ${http}"
else
  if curl -sf "$ready_url" >/dev/null 2>&1; then
    echo "port ${http} is already in use; set REUSE=1 or pick another HTTP=" >&2
    exit 1
  fi
  "$spiced" --http "$http" --flight "$flight" "$workdir" >"$workdir/spiced.log" 2>&1 &
  started_pid=$!
  echo "$started_pid" >"$workdir/spiced.pid"
  trap cleanup EXIT
  echo "started spiced pid=${started_pid} log=$workdir/spiced.log"
  deadline=$((SECONDS + 180))
  until curl -sf "$ready_url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "spiced did not become ready; last log:" >&2
      tail -n 80 "$workdir/spiced.log" >&2
      exit 1
    fi
    if ! kill -0 "$started_pid" 2>/dev/null; then
      echo "spiced exited before ready; log:" >&2
      cat "$workdir/spiced.log" >&2
      exit 1
    fi
    sleep 0.5
  done
fi

sql() {
  local name=$1
  local q=$2
  local out=$workdir/${name}.json
  echo ">> $name: $q"
  curl -sS -X POST "http://${http}/v1/sql" \
    -H 'Accept: application/json' \
    -H 'Content-Type: text/plain' \
    --data-binary "$q" | tee "$out"
  echo
}

decimal='SELECT id, amt/3 AS a3, price/7 AS p7 FROM %s WHERE id IN (1, 5, 8) ORDER BY id'
integer='SELECT n/2 AS half FROM %s WHERE id = 1'
# Safe SQLite price/7 only (no wide-decimal amt). Same row the in-process
# companion asserts; id 1's 0.10/7 is 0.014285 and must not round to 0.014286.
sqlite_price='SELECT id, price/7 AS p7 FROM t_sqlite WHERE id = 1'

sql t_fed "$(printf "$decimal" t_fed)"
sql t_duckdb "$(printf "$decimal" t_duckdb)"
sql t_sqlite "$(printf "$decimal" t_sqlite)"
sql t_sqlite_price "$sqlite_price"
sql t_duckdb_int "$(printf "$integer" t_duckdb)"
sql t_fed_int "$(printf "$integer" t_fed)"
sql t_sqlite_int "$(printf "$integer" t_sqlite)"
sql explain_dec "EXPLAIN SELECT amt/3 FROM t_duckdb WHERE id = 5"
sql explain_int "EXPLAIN SELECT n/2 FROM t_duckdb WHERE id = 1"
sql explain_dec_sqlite "EXPLAIN SELECT amt/3 FROM t_sqlite WHERE id = 5"
sql explain_int_sqlite "EXPLAIN SELECT n/2 FROM t_sqlite WHERE id = 1"

python3 - "$workdir" <<'PY'
import json
import sys
from pathlib import Path

w = Path(sys.argv[1])
failed = []


def raw(name: str) -> str:
    text = (w / f"{name}.json").read_text()
    print(f"--- {name} ---\n{text}")
    return text


def parse_json(name: str, text: str):
    try:
        return json.loads(text)
    except json.JSONDecodeError as err:
        failed.append(f"{name} is not JSON (SQL error?): {err}: {text}")
        return None


def require_rows(name: str, text: str):
    data = parse_json(name, text)
    if data is None:
        return None
    if isinstance(data, dict) and ("error" in data or "message" in data):
        failed.append(f"{name} is an error object: {text}")
        return None
    if not isinstance(data, list):
        failed.append(f"{name} is not a JSON array: {text}")
        return None
    return data


def has_truncating_price(text: str) -> bool:
    compact = text.replace(" ", "")
    return "0.014285" in compact


def has_rounded_price(text: str) -> bool:
    compact = text.replace(" ", "")
    return "0.014286" in compact


def require_half_3(name: str, text: str) -> None:
    rows = require_rows(name, text)
    if rows is None:
        return
    if rows != [{"half": 3}]:
        failed.append(f"{name} integer n/2 was not exactly [{{\"half\":3}}]: {text}")


def plans_from_explain(text: str) -> str:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return text
    if isinstance(data, list):
        parts = [
            row["plan"]
            for row in data
            if isinstance(row, dict) and isinstance(row.get("plan"), str)
        ]
        if parts:
            return "\n".join(parts)
    return text


def base_sql(text: str) -> str | None:
    decoded = plans_from_explain(text)
    marker = "base_sql="
    if marker not in decoded:
        return None
    return decoded.split(marker, 1)[1].split("\n", 1)[0].strip()


def require_base_sql(name: str, text: str) -> str | None:
    pushed = base_sql(text)
    if pushed is None:
        failed.append(f"{name} EXPLAIN has no base_sql=: {text}")
    return pushed


fed = raw("t_fed")
duck = raw("t_duckdb")
half = raw("t_duckdb_int")
explain_dec = raw("explain_dec")
explain_int = raw("explain_int")
# SQLite: SKILL.md requires price/7, integer /, and the EXPLAIN guard.
# Do not compare amt/3 — wide-decimal storage is #14662.
sqlite = raw("t_sqlite")
sqlite_price = raw("t_sqlite_price")
sqlite_half = raw("t_sqlite_int")
explain_dec_sqlite = raw("explain_dec_sqlite")
explain_int_sqlite = raw("explain_int_sqlite")

require_rows("t_fed", fed)
require_rows("t_duckdb", duck)
require_rows("t_sqlite", sqlite)
sqlite_price_rows = require_rows("t_sqlite_price", sqlite_price)
require_half_3("t_duckdb_int", half)
require_half_3("t_sqlite_int", sqlite_half)

if duck != fed:
    failed.append(f"t_duckdb != t_fed\n  fed={fed}\n  duck={duck}")
if "33333333333333333333" not in duck:
    failed.append(f"id5 amt/3 missing exact digits: {duck}")
if "33333333333333333440" in duck:
    failed.append(f"id5 amt/3 still has DOUBLE garbage: {duck}")
if "4115226300411522630" not in duck:
    failed.append(f"id8 amt/3 missing exact digits: {duck}")
if not has_truncating_price(duck):
    failed.append(f"price/7 did not truncate: {duck}")
if has_rounded_price(duck):
    failed.append(f"price/7 rounded instead of truncating: {duck}")

dec_sql = require_base_sql("explain_dec", explain_dec)
if dec_sql is not None:
    if "//" in dec_sql:
        failed.append(f"decimal EXPLAIN still pushes //: {dec_sql}")
    if "amt" not in dec_sql:
        failed.append(f"decimal EXPLAIN did not federate amt: {dec_sql}")
int_sql = require_base_sql("explain_int", explain_int)
if int_sql is not None and "//" not in int_sql:
    failed.append(f"integer EXPLAIN lost //: {int_sql}")

# SQLite price/7: the dedicated id=1 query, and p7 in the full-row body.
# Never require amt/3 digits here (lossy storage, #14662).
if sqlite_price_rows is not None:
    if not has_truncating_price(sqlite_price) or has_rounded_price(sqlite_price):
        failed.append(
            f"SQLite price/7 must truncate (0.014285, not 0.014286): {sqlite_price}"
        )
    p7_values = [
        row.get("p7") for row in sqlite_price_rows if isinstance(row, dict) and "p7" in row
    ]
    if not p7_values:
        failed.append(f"SQLite price/7 response has no p7 field: {sqlite_price}")
if has_rounded_price(sqlite):
    failed.append(f"SQLite full-row price/7 rounded instead of truncating: {sqlite}")

# SQLite EXPLAIN: only the decoded remote SQL. A local ProjectionExec `/`
# must not satisfy the integer-pushdown check.
sqlite_dec_sql = require_base_sql("explain_dec_sqlite", explain_dec_sqlite)
if sqlite_dec_sql is not None:
    if "//" in sqlite_dec_sql or ("amt" in sqlite_dec_sql and "/" in sqlite_dec_sql):
        failed.append(f"SQLite decimal / must stay local; pushed down: {sqlite_dec_sql}")
sqlite_int_sql = require_base_sql("explain_int_sqlite", explain_int_sqlite)
if sqlite_int_sql is not None:
    if "n" not in sqlite_int_sql:
        failed.append(f"SQLite integer EXPLAIN did not federate n: {sqlite_int_sql}")
    elif "/" not in sqlite_int_sql and "//" not in sqlite_int_sql:
        failed.append(f"SQLite integer EXPLAIN lost integer /: {sqlite_int_sql}")

if failed:
    print("VERIFY FAILED:")
    print("\n".join(failed))
    sys.exit(1)
print(
    "VERIFY OK: t_duckdb matches t_fed; exact digits; truncating price/7; "
    "integer / is 3; // not pushed for decimal; SQLite price/7 + integer / + EXPLAIN"
)
PY
