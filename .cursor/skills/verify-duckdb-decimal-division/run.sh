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

ready_url="http://${http}/v1/ready"
if [[ "${REUSE:-}" == 1 ]] && curl -sf "$ready_url" >/dev/null 2>&1; then
  echo "reusing the spiced already listening on ${http}"
else
  if curl -sf "$ready_url" >/dev/null 2>&1; then
    echo "port ${http} is already in use; set REUSE=1 or pick another HTTP=" >&2
    exit 1
  fi
  "$spiced" --http "$http" --flight "$flight" "$workdir" >"$workdir/spiced.log" 2>&1 &
  echo $! >"$workdir/spiced.pid"
  echo "started spiced pid=$(cat "$workdir/spiced.pid") log=$workdir/spiced.log"
  deadline=$((SECONDS + 180))
  until curl -sf "$ready_url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "spiced did not become ready; last log:" >&2
      tail -n 80 "$workdir/spiced.log" >&2
      exit 1
    fi
    if ! kill -0 "$(cat "$workdir/spiced.pid")" 2>/dev/null; then
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

sql t_fed "$(printf "$decimal" t_fed)"
sql t_duckdb "$(printf "$decimal" t_duckdb)"
sql t_sqlite "$(printf "$decimal" t_sqlite)"
sql t_duckdb_int "$(printf "$integer" t_duckdb)"
sql t_fed_int "$(printf "$integer" t_fed)"
sql explain_dec "EXPLAIN SELECT amt/3 FROM t_duckdb WHERE id = 5"
sql explain_int "EXPLAIN SELECT n/2 FROM t_duckdb WHERE id = 1"

python3 - "$workdir" <<'PY'
import sys
from pathlib import Path

w = Path(sys.argv[1])

def raw(name: str) -> str:
    text = (w / f"{name}.json").read_text()
    print(f"--- {name} ---\n{text}")
    return text

fed = raw("t_fed")
duck = raw("t_duckdb")
half = raw("t_duckdb_int")
explain_dec = raw("explain_dec")
explain_int = raw("explain_int")

failed = []
if duck != fed:
    failed.append(f"t_duckdb != t_fed\n  fed={fed}\n  duck={duck}")
if "33333333333333333333" not in duck:
    failed.append(f"id5 amt/3 missing exact digits: {duck}")
if "33333333333333333440" in duck:
    failed.append(f"id5 amt/3 still has DOUBLE garbage: {duck}")
if "4115226300411522630" not in duck:
    failed.append(f"id8 amt/3 missing exact digits: {duck}")
if "0.014285" not in duck:
    failed.append(f"price/7 did not truncate: {duck}")
if "0.014286" in duck:
    failed.append(f"price/7 rounded instead of truncating: {duck}")
if '"half":3' not in half.replace(" ", ""):
    failed.append(f"integer n/2 was not 3: {half}")
if "//" in explain_dec:
    failed.append(f"decimal EXPLAIN still pushes //: {explain_dec}")
if "amt" not in explain_dec:
    failed.append(f"decimal EXPLAIN did not federate amt: {explain_dec}")
if "//" not in explain_int:
    failed.append(f"integer EXPLAIN lost //: {explain_int}")
# Non-vacuous: the decimal scan must still reach DuckDB.
if "base_sql=" not in explain_dec:
    failed.append(f"decimal EXPLAIN has no base_sql=: {explain_dec}")
if failed:
    print("VERIFY FAILED:")
    print("\n".join(failed))
    sys.exit(1)
print("VERIFY OK: t_duckdb matches t_fed; exact digits; truncating price/7; integer / is 3; // not pushed for decimal")
PY

if [[ "${KEEP:-}" != 1 && -f "$workdir/spiced.pid" ]]; then
  kill "$(cat "$workdir/spiced.pid")" 2>/dev/null || true
fi
