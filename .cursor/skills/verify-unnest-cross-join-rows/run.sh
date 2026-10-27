#!/usr/bin/env bash
# Verify that a join against an UNNEST subquery returns every row on each acceleration engine:
# DuckDB and SQLite must agree with Arrow (DataFusion) and with the expected count.
set -euo pipefail

SPICED=${SPICED:-$(command -v spiced || true)}
[[ -x "$SPICED" ]] || { echo "set SPICED to a spiced binary" >&2; exit 2; }
SPICED=$(realpath "$SPICED")
PY=${PY:-python3}
HTTP=${HTTP:-127.0.0.1:18790}
FLIGHT=${FLIGHT:-127.0.0.1:18791}
METRICS=${METRICS:-127.0.0.1:18792}
ROWS=1000
# Only a directory this script created is removed on exit; a WORKDIR the caller names is kept.
if [[ -n ${WORKDIR:-} ]]; then
  mkdir -p "$WORKDIR"
  OWN_WORKDIR=
else
  WORKDIR=$(mktemp -d /tmp/verify-unnest-cross-join-rows.XXXXXX)
  OWN_WORKDIR=1
fi
cd "$WORKDIR"
echo "spiced: $("$SPICED" --version 2>&1 | head -1)"
echo "workdir: $WORKDIR"

"$PY" - "$ROWS" <<'PY'
import sys, pyarrow as pa, pyarrow.parquet as pq
n = int(sys.argv[1])
pq.write_table(pa.table({"id": list(range(n)), "v": [f"v{i}" for i in range(n)]}), "t.parquet")
PY

{
  printf 'version: v1\nkind: Spicepod\nname: verify-unnest-cross-join-rows\ndatasets:\n'
  for e in arrow duckdb sqlite; do
    printf '  - from: file:%s/t.parquet\n    name: t_%s\n    acceleration:\n      enabled: true\n      engine: %s\n' "$WORKDIR" "$e" "$e"
    case $e in
      arrow) printf '      mode: memory\n' ;;
      duckdb) printf '      mode: file\n      params:\n        duckdb_file: %s/t.duckdb\n' "$WORKDIR" ;;
      sqlite) printf '      mode: file\n      params:\n        sqlite_file: %s/t.db\n' "$WORKDIR" ;;
    esac
  done
} > spicepod.yaml

"$SPICED" --http "$HTTP" --flight "$FLIGHT" --metrics "$METRICS" > spiced.log 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; [[ -n ${KEEP:-} || -z $OWN_WORKDIR ]] || rm -rf "$WORKDIR"' EXIT
for _ in $(seq 120); do
  [[ $(grep -c "Dataset t_.* registered" spiced.log || true) -ge 3 ]] && break
  kill -0 $PID 2>/dev/null || { cat spiced.log; exit 1; }
  sleep 1
done
sleep 2

q() { curl -s -XPOST "http://$HTTP/v1/sql" -H 'Content-Type: text/plain' -H 'Accept: text/csv' --data "$1" | tail -n +2 | tr -d '\r'; }

FAIL=0
check() { # name expected sql-with-__T__
  for e in arrow duckdb sqlite; do
    local got; got=$(q "${3//__T__/t_$e}")
    local verdict=PASS; [[ $got == "$2" ]] || { verdict=FAIL; FAIL=1; }
    printf '%-7s %-58s got=%-10s expected=%-7s %s\n' "$e" "$1" "${got:0:60}" "$2" "$verdict"
  done
}
check "count(*) cross join (select * from unnest(range(0,100)))" $((ROWS * 100)) \
  "select count(*) from __T__ a cross join (select * from unnest(range(0, 100))) b"
check "rows of a.* cross join unnest(range(0,100))" $((ROWS * 100)) \
  "select count(*) from (select a.* from __T__ a cross join unnest(range(0, 100)) b)"
check "count(*) cross join (select unnest([1,2,3]) as x)" $((ROWS * 3)) \
  "select count(*) from __T__ a cross join (select unnest([1, 2, 3]) as x) b"
check "join on b.x of (select unnest([1,2,3]) as x)" "$ROWS" \
  "select count(*) from __T__ a join (select unnest([1, 2, 3]) as x) b on a.id % 3 + 1 = b.x"
check "control: cross join generate_series(1,3)" $((ROWS * 3)) \
  "select count(*) from __T__ a cross join generate_series(1, 3) b"

echo "-- SQL sent to t_duckdb for the first check:"
q "explain select count(*) from t_duckdb a cross join (select * from unnest(range(0, 100))) b" | grep -o 'sql=[^,]*' | head -3 || true
[[ $FAIL == 0 ]] && echo "RESULT: PASS" || { echo "RESULT: FAIL"; exit 1; }
