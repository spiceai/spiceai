#!/usr/bin/env bash
# Verify acceleration primary_key / unique index on non-identifier column names.
set -euo pipefail

SPICED=${SPICED:-$(command -v spiced || true)}
[[ -x "$SPICED" ]] || { echo "set SPICED to a spiced binary" >&2; exit 2; }
HTTP=${HTTP:-127.0.0.1:18590}
FLIGHT=${FLIGHT:-127.0.0.1:18591}
METRICS=${METRICS:-127.0.0.1:18592}
ENGINES=${ENGINES:-duckdb sqlite arrow cayenne}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/verify-constraint-names.XXXXXX)}
cd "$WORKDIR"
echo "workdir: $WORKDIR"

printf 'service.instance.id,sentry-environment,2xx_count,a\ns1,prod,10,1\ns2,dev,20,4\n' > data.csv

cols=("service.instance.id" "sentry-environment" "2xx_count")
slug=(dot hyphen digit)
{
  echo "version: v1"
  echo "kind: Spicepod"
  echo "name: verify-constraint-column-names"
  echo "datasets:"
  for e in $ENGINES; do
    for i in 0 1 2; do
      for q in b q; do
        c=${cols[$i]}
        [[ $q == q ]] && key="\"$c\"" || key="$c"
        echo "  - from: file:$WORKDIR/data.csv"
        echo "    name: t_${e}_${slug[$i]}_${q}"
        echo "    acceleration:"
        echo "      enabled: true"
        echo "      engine: $e"
        echo "      primary_key: '$key'"
        echo "      indexes:"
        echo "        '$key': unique"
      done
    done
    echo "  - from: file:$WORKDIR/data.csv"
    echo "    name: t_${e}_composite"
    echo "    acceleration:"
    echo "      enabled: true"
    echo "      engine: $e"
    echo "      primary_key: '(\"service.instance.id\", \"sentry-environment\", \"2xx_count\")'"
  done
} > spicepod.yaml

"$SPICED" --http "$HTTP" --flight "$FLIGHT" --metrics "$METRICS" > spiced.log 2>&1 &
PID=$!
[[ "${KEEP:-}" == 1 ]] || trap 'kill $PID 2>/dev/null || true' EXIT

names=$(grep -E '^    name: ' spicepod.yaml | awk '{print $2}')
total=$(echo "$names" | wc -l)
for _ in $(seq 1 60); do
  sleep 2
  st=$(curl -sf "http://$HTTP/v1/datasets?status=true" || true)
  ready=$(grep -o '"status":"Ready"' <<<"$st" | wc -l || true)
  errored=$(grep -oE '"status":"Error"' <<<"$st" | wc -l || true)
  (( ready + errored >= total )) && break
done
echo "$st" > datasets.json

fail=0
for n in $names; do
  s=$(grep -oE "\"name\":\"$n\"[^}]*\"status\":\"[A-Za-z]+\"" datasets.json | grep -oE '"status":"[A-Za-z]+"' | cut -d'"' -f4 || true)
  if [[ "$s" != Ready ]]; then
    echo "FAIL $n status=${s:-missing}"; fail=1; continue
  fi
  c=$(curl -sf "http://$HTTP/v1/sql" -H 'Content-Type: text/plain' -d "SELECT count(*) AS c FROM $n" | grep -oE '"c":[0-9]+' | cut -d: -f2 || true)
  if [[ "$c" != 2 ]]; then echo "FAIL $n count=${c:-error}"; fail=1; else echo "ok   $n"; fi
done

if grep -qE 'Parser Error|syntax error at or near' spiced.log; then
  echo "FAIL log has a parser error:"; grep -E 'Parser Error|syntax error at or near' spiced.log | cut -c1-300 | sort -u | head; fail=1
fi
[[ $fail == 0 ]] && echo "PASS ($total datasets)" || echo "FAILED (log: $WORKDIR/spiced.log)"
exit $fail
