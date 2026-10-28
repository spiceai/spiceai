#!/usr/bin/env bash
# Verify that DuckDB federation answers each probe query exactly as DataFusion
# does. The same CSV is registered twice: `fed` (duckdb:read_csv, so the query
# is unparsed and pushed to DuckDB) and `ref` (file:, evaluated by DataFusion).
# The script exits non-zero when any probe disagrees, or when the pushed SQL of a
# probe still carries a construct that has to stay local.
set -euo pipefail

SPICED=${SPICED:-$(command -v spiced || echo target/debug/spiced)}
HTTP=${HTTP:-127.0.0.1:18590}
FLIGHT=${FLIGHT:-127.0.0.1:18591}
METRICS=${METRICS:-127.0.0.1:18592}
W=${WORKDIR:-$(mktemp -d /tmp/verify-pushdown.XXXX)}
PID=

cleanup() {
  if [[ ${KEEP:-0} != 1 ]]; then
    [[ -n $PID ]] && kill "$PID" 2>/dev/null || true
    [[ -z ${WORKDIR:-} ]] && rm -rf "$W"
  fi
}
trap cleanup EXIT

printf 'k,s,x,n\n1,abc,1.5,-7\n2,a%%c,2.5,7\n3,cust_12,-0.5,0\n4,ZOË,0.0,3\n' > "$W/t.csv"
cat > "$W/spicepod.yaml" <<YAML
version: v1
kind: Spicepod
name: verify-federation-pushdown-semantics
datasets:
  - from: duckdb:read_csv('$W/t.csv')
    name: fed
  - from: file:$W/t.csv
    name: ref
    params:
      file_format: csv
YAML

if [[ ${REUSE:-0} != 1 ]]; then
  (cd "$W" && exec "$SPICED" --http "$HTTP" --flight "$FLIGHT" --metrics "$METRICS" > "$W/spiced.log" 2>&1) &
  PID=$!
fi
for _ in $(seq 1 120); do curl -sf "$HTTP/v1/ready" >/dev/null 2>&1 && break; sleep 1; done
curl -sf "$HTTP/v1/ready" >/dev/null || { echo "spiced did not become ready; log: $W/spiced.log" >&2; exit 2; }

q() { curl -s "$HTTP/v1/sql" -H 'content-type: text/plain' -d "$1"; }

# Every probe must answer on both routes with the same JSON array, byte for
# byte. A probe DataFusion fails by design is written with `probe_error`: both
# routes must then fail with that error, so an unrelated failure (a planning
# error, a missing table) on both never counts as agreement.
fail=0
run_both() {
  local sql=$1
  fed=$(q "${sql//\{T\}/fed}")
  ref=$(q "${sql//\{T\}/ref}")
  echo "== $sql"
  echo "fed: $fed"
  echo "ref: $ref"
}
probe() {
  local fed ref
  run_both "$1"
  if [[ $ref != \[* || $fed != \[* || $fed != "$ref" ]]; then
    echo "MISMATCH"
    fail=1
  fi
}
probe_error() {
  local fed ref expected=$2
  run_both "$1"
  if [[ $ref == \[* || $fed == \[* || $ref != *"$expected"* || $fed != *"$expected"* ]]; then
    echo "MISMATCH (both routes must fail with: $expected)"
    fail=1
  fi
}

probe "select count(*) AS similar_to_hits from {T} where s similar to 'cust_[0-9]+'"
probe "select count(*) AS not_similar_hits from {T} where s not similar to 'cust_[0-9]+'"
probe "select k, s like 'a\\%c' AS like_backslash_escape from {T} order by k"
probe "select k, s ilike 'A\\%C' AS ilike_backslash_escape from {T} order by k"
probe "select k, s like 'a%c' AS like_plain from {T} order by k"
probe "select k, substr(s, -2, 4) AS substr_neg from {T} order by k"
probe "select k, substr(s, 0, 2) AS substr_zero from {T} order by k"
probe "select k, substr(s, 2, 2) AS substr_pos from {T} order by k"
probe "select count(*) AS x_div0_is_null from {T} where x / 0 is null"
probe "select count(*) AS x_div0_gt_1 from {T} where x / 0 > 1"
probe_error "select count(*) AS n_div_k_minus_3 from {T} where n / (k - 3) > 0" 'Divide by zero'
probe_error "select count(*) AS n_mod0 from {T} where n % 0 is null" 'Divide by zero'
probe "select k, n / 2 AS half, n % 2 AS rem from {T} order by k"
probe "select to_char(now(), '%Y') AS to_char_y from {T} limit 1"
probe "select k, to_unixtime(concat('2026-01-0', cast(k as varchar))) AS unix from {T} order by k"
probe "select k, cardinality(make_array(1,2)) AS card from {T} order by k limit 1"

# Pushed SQL guards: SIMILAR TO must not reach DuckDB rendered as LIKE, and a
# literal-safe division must still push.
base_sql() { q "explain $1" | python3 -c 'import sys,json,re; print("\n".join(m for r in json.load(sys.stdin) for m in re.findall(r"base_sql=.*", r["plan"])))'; }
similar=$(base_sql "select count(*) from fed where s similar to 'cust_[0-9]+'")
echo "== pushed SQL, SIMILAR TO: ${similar:-<none>}"
if grep -q "LIKE 'cust_\[0-9\]+'" <<<"$similar"; then echo "MISMATCH: SIMILAR TO pushed as LIKE"; fail=1; fi
halves=$(base_sql "select n / 2 from fed")
echo "== pushed SQL, n / 2: ${halves:-<none>}"
if ! grep -q '//' <<<"$halves"; then echo "MISMATCH: literal-safe integer division no longer pushes"; fail=1; fi

if [[ $fail == 0 ]]; then echo "PASS: every probe agrees"; else echo "FAIL"; fi
exit $fail
