#!/usr/bin/env bash
#
# Pick host ports for PostgreSQL, MySQL, and ClickHouse for this runner
# instance. Several instances share one machine and one network namespace, so
# the stock ports 5432, 3306, and 9001 cannot be used by two jobs at once.
# Each window is SPAN wide and sits below the Spice HTTP window (20000) and
# below the Linux ephemeral range (32768):
#   10000 PostgreSQL
#   11000 MySQL
#   12000 ClickHouse native protocol
#   13000 ClickHouse HTTP
#   14000 ClickHouse MySQL protocol
#   15000 ClickHouse PostgreSQL protocol
#   16000 ClickHouse inter-server HTTP
# ClickHouse's packaged config.xml listens on all five of those, so a second
# server that only moves the native port still fails to bind 8123, 9004, 9005,
# or 9009.
#
# Each port handed out is reserved with an atomic `mkdir` under LOCK_DIR, which
# every instance on the machine shares. A listener probe alone is not enough:
# two instances whose seeds land on the same offset both see the port free
# while their databases are still installing, and both bind it — and both own
# the data directory named after it. The `mkdir` is the only way a port is
# granted, and a port whose reservation exists is never handed out, however old
# it is. Reservations still owned by this runner instance are released first:
# an instance runs one job at a time, so they are leftovers of its previous job
# and nothing else can be using them. One left by an instance that never runs
# again stays until /tmp is cleared, and costs that one port.
# release_ports.sh (from stop-services) frees this job's reservations. When a
# window has no port free within MAX_PROBE tries, this fails without exporting
# anything rather than hand out a port another job may hold.

set -uo pipefail

PG_BASE="${CI_PG_PORT_BASE:-10000}"
MYSQL_BASE="${CI_MYSQL_PORT_BASE:-11000}"
CLICKHOUSE_BASE="${CI_CLICKHOUSE_PORT_BASE:-12000}"
CLICKHOUSE_HTTP_BASE="${CI_CLICKHOUSE_HTTP_PORT_BASE:-13000}"
CLICKHOUSE_MYSQL_BASE="${CI_CLICKHOUSE_MYSQL_PORT_BASE:-14000}"
CLICKHOUSE_PG_BASE="${CI_CLICKHOUSE_PG_PORT_BASE:-15000}"
CLICKHOUSE_INTERSERVER_BASE="${CI_CLICKHOUSE_INTERSERVER_PORT_BASE:-16000}"
SPAN="${CI_SERVICE_PORT_SPAN:-1000}"
MAX_PROBE="${CI_SERVICE_PORT_MAX_PROBE:-25}"
LOCK_DIR="${CI_SERVICE_PORT_LOCK_DIR:-/tmp/spice-ci-service-ports}"
owner="${RUNNER_NAME:-pid-$$}"
mkdir -p "${LOCK_DIR}"

seed_source="${RUNNER_NAME:-}|${GITHUB_RUN_ID:-}|${GITHUB_JOB:-}|${GITHUB_RUN_ATTEMPT:-}"

seed_hash() {
  printf '%s' "$1" | cksum | awk '{print $1}'
}

port_in_use() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  if command -v netstat >/dev/null 2>&1; then
    netstat -an 2>/dev/null | grep -E "[.:]${port}[[:space:]]+.*LISTEN" >/dev/null 2>&1
    return $?
  fi
  return 1
}

reserve_port() {
  local lock="${LOCK_DIR}/$1"
  mkdir "${lock}" 2>/dev/null || return 1
  printf '%s\n' "${owner}" >"${lock}/owner"
}

release_reserved() {
  local port
  for port in "$@"; do
    rm -rf "${LOCK_DIR:?}/${port}"
  done
}

pick_port() {
  local base="$1"
  local offset="$2"
  local probe=0
  local port
  while :; do
    port=$((base + (offset + probe) % SPAN))
    if ! port_in_use "${port}" && reserve_port "${port}"; then
      printf '%s' "${port}"
      return 0
    fi
    probe=$((probe + 1))
    if [ "${probe}" -ge "${MAX_PROBE}" ]; then
      echo "::error::No free port within ${MAX_PROBE} tries of ${base}+${offset}: each is listening or reserved by another job in ${LOCK_DIR}." >&2
      return 1
    fi
  done
}

for lock in "${LOCK_DIR}"/*; do
  if [ "$(cat "${lock}/owner" 2>/dev/null)" = "${owner}" ]; then
    rm -rf "${lock}"
  fi
done

hash_value="$(seed_hash "${seed_source}")"
if [ -z "${hash_value}" ]; then
  hash_value=$$
fi
offset=$((hash_value % SPAN))

reserved=()
for base in "${PG_BASE}" "${MYSQL_BASE}" "${CLICKHOUSE_BASE}" "${CLICKHOUSE_HTTP_BASE}" \
  "${CLICKHOUSE_MYSQL_BASE}" "${CLICKHOUSE_PG_BASE}" "${CLICKHOUSE_INTERSERVER_BASE}"; do
  if ! port="$(pick_port "${base}" "${offset}")"; then
    release_reserved "${reserved[@]+"${reserved[@]}"}"
    exit 1
  fi
  reserved+=("${port}")
done
pg_port="${reserved[0]}"
mysql_port="${reserved[1]}"
clickhouse_port="${reserved[2]}"
clickhouse_http_port="${reserved[3]}"
clickhouse_mysql_port="${reserved[4]}"
clickhouse_pg_port="${reserved[5]}"
clickhouse_interserver_port="${reserved[6]}"

echo "Service ports for runner '${RUNNER_NAME:-unknown}': postgres ${pg_port}, mysql ${mysql_port}, clickhouse ${clickhouse_port} (http ${clickhouse_http_port})."

assignments="CI_PG_PORT=${pg_port}
CI_MYSQL_PORT=${mysql_port}
CI_CLICKHOUSE_PORT=${clickhouse_port}
CI_CLICKHOUSE_HTTP_PORT=${clickhouse_http_port}
CI_CLICKHOUSE_MYSQL_PORT=${clickhouse_mysql_port}
CI_CLICKHOUSE_PG_PORT=${clickhouse_pg_port}
CI_CLICKHOUSE_INTERSERVER_PORT=${clickhouse_interserver_port}
CI_SERVICE_PORT_LOCKS=${reserved[*]}"
printf '%s\n' "${assignments}"
if [ -n "${GITHUB_ENV:-}" ]; then
  printf '%s\n' "${assignments}" >>"${GITHUB_ENV}"
fi
