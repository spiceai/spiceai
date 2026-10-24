#!/usr/bin/env bash
# Start a local spiced on the overflow spicepod and print HTTP bodies for the
# three success-criteria queries against t_fed, t_arrow, and t_cayenne.
set -euo pipefail

ROOT="${1:-/tmp/integer-overflow-verify}"
SPICED="${SPICED:-$(command -v spiced || true)}"
HTTP="${HTTP:-127.0.0.1:18490}"
FLIGHT="${FLIGHT:-127.0.0.1:18491}"
PYTHON="${PYTHON:-python3}"

if [[ -z "${SPICED}" || ! -x "${SPICED}" ]]; then
  echo "SPICED is not set to an executable spiced binary" >&2
  exit 1
fi

# Resolve before `cd "${ROOT}"`. A relative path such as `target/debug/spiced`
# would otherwise miss after the chdir (exit 127).
if resolved="$(realpath "${SPICED}" 2>/dev/null)"; then
  SPICED="${resolved}"
else
  SPICED="$(cd "$(dirname -- "${SPICED}")" && pwd)/$(basename -- "${SPICED}")"
fi

mkdir -p "${ROOT}/data"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp "${SCRIPT_DIR}/../spicepod.yaml" "${ROOT}/spicepod.yaml"

"${PYTHON}" "${SCRIPT_DIR}/gen_overflow_parquet.py" "${ROOT}/data/overflow.parquet"

cd "${ROOT}"

# Refuse a leftover listener. This helper leaves its server running, so a
# second trunk/branch run on the same HTTP bind would query that instance and
# attribute the bodies to this SPICED.
if existing="$(curl -fsS --max-time 1 "http://${HTTP}/v1/ready" 2>/dev/null)"; then
  echo "HTTP ${HTTP} is already responding (body=${existing}); not starting ${SPICED}." >&2
  echo "Stop that process or set HTTP and FLIGHT to free ports." >&2
  exit 1
fi

rm -f spiced.log spiced.pid
"${SPICED}" --http "${HTTP}" --flight "${FLIGHT}" --metrics 127.0.0.1:0 >spiced.log 2>&1 &
echo $! >spiced.pid
SPICED_PID="$(cat spiced.pid)"
# Leave this spiced running so a follow-up can query the same instance.

ready=""
last="not contacted"
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  if ! kill -0 "${SPICED_PID}" 2>/dev/null; then
    echo "spiced pid ${SPICED_PID} exited before ready; last 80 log lines:" >&2
    tail -n 80 spiced.log >&2 || true
    exit 1
  fi
  # Bound each probe so a server that accepts and never replies cannot
  # stall the loop past `deadline`.
  if last="$(curl -fsS --connect-timeout 1 --max-time 2 "http://${HTTP}/v1/ready" 2>/dev/null || true)"; then
    if [[ "${last}" == "ready" ]]; then
      ready="yes"
      break
    fi
  fi
  sleep 0.25
done

if [[ -z "${ready}" ]]; then
  echo "timed out waiting for /v1/ready; last body: ${last}" >&2
  echo "datasets:" >&2
  curl -sS --connect-timeout 1 --max-time 2 "http://${HTTP}/v1/datasets?status=true" >&2 || true
  echo "last 80 log lines:" >&2
  tail -n 80 spiced.log >&2 || true
  exit 1
fi

echo "SHA=$(git -C /workspace rev-parse HEAD 2>/dev/null || echo unknown)"
echo "SPICED=${SPICED}"
echo "ready=${last}"
echo

sql() {
  local q="$1"
  echo ">> ${q}"
  local tmp status
  tmp="$(mktemp)"
  status="$(curl -sS --connect-timeout 1 --max-time 30 -o "${tmp}" -w '%{http_code}' -X POST "http://${HTTP}/v1/sql" --data "${q}" || true)"
  echo "HTTP ${status}"
  cat "${tmp}"
  rm -f "${tmp}"
  echo
  echo
}

for table in t_fed t_arrow t_cayenne; do
  sql "SELECT SUM(big) FROM ${table}"
  sql "SELECT big + 1 FROM ${table} WHERE id = 1"
  sql "SELECT SUM(big) FROM ${table} WHERE id IN (3, 4)"
done
