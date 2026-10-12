#!/usr/bin/env bash
# Release gate: start the built spiced on the release-notes highlight spicepod and
# require every connector to be registered and every dataset to reach Ready.
# Usage: test/release/smoke_highlights.sh [path/to/spiced]
set -euo pipefail
SPICED=$(realpath "${1:-spiced}")
TIMEOUT=${SMOKE_TIMEOUT_SECS:-300}
POD_DIR="$(cd "$(dirname "$0")/highlights" && pwd)"
LOG=$(mktemp)
"$SPICED" --version
cd "$POD_DIR"
"$SPICED" --http 127.0.0.1:8090 >"$LOG" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true; rm -rf "$POD_DIR/.spice"' EXIT
expected=$(grep -c '^  - from:' spicepod.yaml)
deadline=$((SECONDS + TIMEOUT))
while (( SECONDS < deadline )); do
  if grep -q "Unknown data connector" "$LOG"; then
    grep "Unknown data connector" "$LOG"; echo "FAIL: a highlighted connector is not built into this binary"; exit 1
  fi
  status=$(curl -sf 'http://127.0.0.1:8090/v1/datasets?status=true' || true)
  if [[ -n "$status" ]]; then
    ready=$(jq '[.[] | select(.status == "Ready")] | length' <<<"$status")
    echo "ready ${ready}/${expected}"
    if (( ready == expected )); then echo "PASS: all ${expected} highlight datasets Ready"; exit 0; fi
  fi
  sleep 5
done
echo "FAIL: not all datasets Ready within ${TIMEOUT}s"
curl -sf 'http://127.0.0.1:8090/v1/datasets?status=true' | jq -c '.[] | {name, status}' || true
tail -50 "$LOG"
exit 1
