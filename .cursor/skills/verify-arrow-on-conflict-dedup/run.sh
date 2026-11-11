#!/usr/bin/env bash
# Verify acceleration on_conflict upsert_dedup / upsert_dedup_by_row_id / drop resolve duplicate
# primary keys within one load the same way on every engine.
set -euo pipefail

SPICED=${SPICED:-$(command -v spiced || true)}
[[ -x "$SPICED" ]] || { echo "set SPICED to a spiced binary" >&2; exit 2; }
SPICED=$(realpath "$SPICED")  # the script changes directory below
HTTP=${HTTP:-127.0.0.1:18690}
FLIGHT=${FLIGHT:-127.0.0.1:18691}
METRICS=${METRICS:-127.0.0.1:18692}
ENGINES=${ENGINES:-arrow duckdb}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/verify-arrow-on-conflict.XXXXXX)}
cd "$WORKDIR"
echo "workdir: $WORKDIR"

# exact.csv: an exact duplicate row. two.csv: two different rows share id 1 (first 1,a, last 1,z).
printf 'id,val\n1,a\n1,a\n2,b\n' > exact.csv
printf 'id,val\n1,a\n1,z\n2,b\n' > two.csv

# case name | file | on_conflict | expected ("ERROR" or the rows as id,val;id,val)
cases=(
  "dedup_exact|exact.csv|upsert_dedup|1,a;2,b"
  "dedup_two|two.csv|upsert_dedup|ERROR"
  "by_row_id_exact|exact.csv|upsert_dedup_by_row_id|1,a;2,b"
  "by_row_id_two|two.csv|upsert_dedup_by_row_id|1,z;2,b"
  "drop_exact|exact.csv|drop|1,a;2,b"
  "drop_two|two.csv|drop|1,a;2,b"
)
{
  echo "version: v1"
  echo "kind: Spicepod"
  echo "name: verify-arrow-on-conflict-dedup"
  echo "datasets:"
  for e in $ENGINES; do
    for c in "${cases[@]}"; do
      IFS='|' read -r name file oc _ <<<"$c"
      echo "  - from: file:$WORKDIR/$file"
      echo "    name: t_${e}_${name}"
      echo "    acceleration:"
      echo "      enabled: true"
      echo "      engine: $e"
      echo "      primary_key: id"
      echo "      on_conflict:"
      echo "        id: $oc"
    done
  done
} > spicepod.yaml

"$SPICED" --http "$HTTP" --flight "$FLIGHT" --metrics "$METRICS" > spiced.log 2>&1 &
PID=$!
[[ "${KEEP:-}" == 1 ]] || trap 'kill $PID 2>/dev/null || true' EXIT

total=$(grep -cE '^    name: ' spicepod.yaml)
for _ in $(seq 1 60); do
  sleep 2
  # Never accept answers from another instance left on these ports.
  kill -0 "$PID" 2>/dev/null || { echo "FAIL spiced exited early (log: $WORKDIR/spiced.log)"; tail -5 spiced.log >&2; exit 1; }
  st=$(curl -sf "http://$HTTP/v1/datasets?status=true" || true)
  done_count=$(grep -oE '"status":"(Ready|Error)"' <<<"$st" | wc -l || true)
  (( done_count >= total )) && break
done
echo "$st" > datasets.json

fail=0
for e in $ENGINES; do
  for c in "${cases[@]}"; do
    IFS='|' read -r name _ _ want <<<"$c"
    n="t_${e}_${name}"
    s=$(python3 -c "import json,sys;print(next((d['status'] for d in json.load(open('datasets.json')) if d['name']==sys.argv[1]),'missing'))" "$n")
    if [[ "$s" == Ready ]]; then
      got=$(curl -sf "http://$HTTP/v1/sql" -H 'Content-Type: text/plain' \
        -d "SELECT CAST(id AS VARCHAR) || ',' || val AS r FROM $n ORDER BY id" |
        python3 -c "import json,sys;print(';'.join(r['r'] for r in json.load(sys.stdin)))" || echo "query-error")
    else
      got="ERROR"
    fi
    if [[ "$got" == "$want" ]]; then
      echo "ok   $n -> $got"
    else
      echo "FAIL $n want=$want got=$got (status=$s)"; fail=1
    fi
  done
done

echo "--- load errors / warnings"
grep -E 'ERROR|WARN' spiced.log | grep -E 'unique|on_conflict|Primary key|ignored' | grep -v deprecated | cut -c1-260 | sort -u | head -20 || true
[[ $fail == 0 ]] && echo "PASS" || echo "FAILED (log: $WORKDIR/spiced.log)"
exit $fail
