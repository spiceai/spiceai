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

pick_port() {
  local base="$1"
  local offset="$2"
  local probe=0
  local port
  while :; do
    port=$((base + (offset + probe) % SPAN))
    if ! port_in_use "${port}"; then
      printf '%s' "${port}"
      return 0
    fi
    probe=$((probe + 1))
    if [ "${probe}" -ge "${MAX_PROBE}" ]; then
      echo "::warning::Could not find a free port within ${MAX_PROBE} tries of ${base}+${offset}; using ${port} anyway." >&2
      printf '%s' "${port}"
      return 0
    fi
  done
}

hash_value="$(seed_hash "${seed_source}")"
if [ -z "${hash_value}" ]; then
  hash_value=$$
fi
offset=$((hash_value % SPAN))

pg_port="$(pick_port "${PG_BASE}" "${offset}")"
mysql_port="$(pick_port "${MYSQL_BASE}" "${offset}")"
clickhouse_port="$(pick_port "${CLICKHOUSE_BASE}" "${offset}")"
clickhouse_http_port="$(pick_port "${CLICKHOUSE_HTTP_BASE}" "${offset}")"
clickhouse_mysql_port="$(pick_port "${CLICKHOUSE_MYSQL_BASE}" "${offset}")"
clickhouse_pg_port="$(pick_port "${CLICKHOUSE_PG_BASE}" "${offset}")"
clickhouse_interserver_port="$(pick_port "${CLICKHOUSE_INTERSERVER_BASE}" "${offset}")"

echo "Service ports for runner '${RUNNER_NAME:-unknown}': postgres ${pg_port}, mysql ${mysql_port}, clickhouse ${clickhouse_port} (http ${clickhouse_http_port})."

assignments="CI_PG_PORT=${pg_port}
CI_MYSQL_PORT=${mysql_port}
CI_CLICKHOUSE_PORT=${clickhouse_port}
CI_CLICKHOUSE_HTTP_PORT=${clickhouse_http_port}
CI_CLICKHOUSE_MYSQL_PORT=${clickhouse_mysql_port}
CI_CLICKHOUSE_PG_PORT=${clickhouse_pg_port}
CI_CLICKHOUSE_INTERSERVER_PORT=${clickhouse_interserver_port}"
printf '%s\n' "${assignments}"
if [ -n "${GITHUB_ENV:-}" ]; then
  printf '%s\n' "${assignments}" >>"${GITHUB_ENV}"
fi
