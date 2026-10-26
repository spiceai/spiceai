#!/usr/bin/env bash
# Wait until the spice-mssql-bench service container's TPC-H database is ONLINE.
#
# The image's /srv/setup.sh sleeps a fixed 30 seconds after starting SQL Server,
# then issues one RESTORE and never retries it. When SQL Server takes longer than
# that to accept connections, the RESTORE fails with "Login timeout expired" and
# the database never exists. The service has no health check, so the job's steps
# start at once and spiced waits on a database that will never appear.
#
# This waits for setup.sh's attempt to finish, restores the database here if that
# attempt failed, and fails with the reason if it is not ONLINE by the deadline.
#
# Usage: bash .github/scripts/mssql_tpch_restore.sh CONTAINER_ID DB_NAME
# MSSQL_SA_PASSWORD is the sa password. MSSQL_RESTORE_TIMEOUT_SECONDS (default
# 600) bounds the whole wait, and the restore's query timeout is what is left of
# it. MSSQL_RESTORE_POLL_SECONDS (default 5) is the pause between checks.
set -euo pipefail

if [ "$#" -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
  echo "::error::Usage: mssql_tpch_restore.sh CONTAINER_ID DB_NAME"
  exit 2
fi
container="$1"
db="$2"
# The name is spliced into T-SQL and a file path below.
if ! [[ "$db" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "::error::DB_NAME must be letters, digits and underscores, got '$db'."
  exit 2
fi
if [ -z "${MSSQL_SA_PASSWORD:-}" ]; then
  echo "::error::MSSQL_SA_PASSWORD is not set."
  exit 2
fi
timeout="${MSSQL_RESTORE_TIMEOUT_SECONDS:-600}"
poll="${MSSQL_RESTORE_POLL_SECONDS:-5}"
if ! [[ "$timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "::error::MSSQL_RESTORE_TIMEOUT_SECONDS must be a whole number of at least 1, got '$timeout'."
  exit 2
fi
if ! [[ "$poll" =~ ^[0-9]+$ ]]; then
  echo "::error::MSSQL_RESTORE_POLL_SECONDS must be a whole number, got '$poll'."
  exit 2
fi
deadline=$((SECONDS + timeout))

# Runs one query with a query timeout of $1 seconds; `-l` bounds only the login.
# The password travels in the environment, not on the command line.
sqlcmd() {
  local query_timeout="$1"
  shift
  SQLCMDPASSWORD="$MSSQL_SA_PASSWORD" docker exec -e SQLCMDPASSWORD "$container" \
    /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -l 5 -t "$query_timeout" -b -h -1 -W -d master "$@"
}

# Seconds left before the deadline, at least 1.
remaining() {
  local left=$((deadline - SECONDS))
  echo $((left > 1 ? left : 1))
}

# Prints ONLINE, RESTORING, ... or nothing when the database does not exist or
# SQL Server is not accepting connections yet.
db_state() {
  local out
  out="$(sqlcmd 30 -Q "SET NOCOUNT ON; SELECT state_desc FROM sys.databases WHERE name = N'$db';" 2>/dev/null)" || return 0
  printf '%s' "$out" | tr -d '[:space:]'
}

give_up() {
  echo "::error::$1"
  echo "--- last 50 lines of the SQL Server container log ---"
  docker logs --tail 50 "$container" 2>&1 || true
  exit 1
}

# setup.sh's RESTORE has ended in failure once sqlcmd reports a connection error
# or SQL Server returns an error message to it.
restore_failed='Sqlcmd: Error|Msg [0-9]+, Level [0-9]+'

while :; do
  state="$(db_state)"
  if [ "$state" = ONLINE ]; then
    echo "$db is ONLINE."
    exit 0
  fi
  logs="$(docker logs "$container" 2>&1)" || logs=''
  if [ -z "$state" ] && grep -Eq "$restore_failed" <<<"$logs"; then
    break
  fi
  if ((SECONDS >= deadline)); then
    give_up "$db was not ONLINE within ${timeout}s (state: '${state:-absent}'), and the image's restore has not reported a failure."
  fi
  sleep "$poll"
done

echo "::warning::The image's restore of $db failed, most likely because SQL Server was not yet accepting connections when it ran. Restoring /data/$db.bak here."
until sqlcmd 30 -Q 'SELECT 1' >/dev/null 2>&1; do
  if ((SECONDS >= deadline)); then
    give_up "SQL Server did not accept connections within ${timeout}s."
  fi
  sleep "$poll"
done
if ! sqlcmd "$(remaining)" -Q "RESTORE DATABASE [$db] FROM DISK = N'/data/$db.bak' WITH REPLACE"; then
  give_up "RESTORE DATABASE $db from /data/$db.bak failed."
fi
state="$(db_state)"
if [ "$state" != ONLINE ]; then
  give_up "$db is '${state:-absent}' after the restore, expected ONLINE."
fi
echo "$db is ONLINE."
