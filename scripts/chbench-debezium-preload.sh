#!/usr/bin/env bash
# Preload the dedicated CH-benCH Redpanda/Debezium stack with a full Debezium
# snapshot from the long-lived chbench Postgres service. This is intentionally
# outside the benchmark run: the v1.11 run still loads the full baseline from
# Debezium/Kafka, but connector snapshot generation is pre-created infra state.
set -euo pipefail

NS="${NAMESPACE:-dataplatform}"
SF="${SCALE_FACTOR:-100}"
PREFIX="${CHBENCH_DEBEZIUM_TOPIC_PREFIX:-chbench_sf${SF}_v111_preloaded}"
PG_HOST="${CHBENCH_PG_HOST:-chbench-postgres.${NS}.svc.cluster.local}"
PG_PORT="${CHBENCH_PG_PORT:-5432}"
# Use an isolated source DB so unrelated CH-benCH runs can freely drop/restore
# the shared `chbench` database while this long Debezium snapshot is running.
PG_DB="${CHBENCH_PG_DB:-chbench_debezium_sf${SF}}"
PG_TEMPLATE_DB="${CHBENCH_PG_TEMPLATE_DB:-chbench_tmpl_sf${SF}}"
PG_USER="${CHBENCH_PG_USER:-bench}"
PG_PASS="${CHBENCH_PG_PASS:-bench}"
CONNECT_SERVICE="${CHBENCH_DEBEZIUM_SERVICE:-svc/chbench-debezium}"
REDPANDA_POD="${CHBENCH_REDPANDA_POD:-chbench-redpanda-0}"
CONNECT_LOCAL_PORT="${CHBENCH_CONNECT_LOCAL_PORT:-18083}"
CONNECT_URL="http://127.0.0.1:${CONNECT_LOCAL_PORT}"
TABLES=(customer district new_order order_line oorder stock warehouse)

tmpdir="$(mktemp -d)"
pf_log="${tmpdir}/port-forward.log"
pf_pid=""
cleanup() {
  if [[ -n "${pf_pid}" ]]; then
    kill "${pf_pid}" 2>/dev/null || true
    wait "${pf_pid}" 2>/dev/null || true
  fi
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

ident() {
  python3 - "$1" <<'PY'
import re, sys
s = re.sub(r'[^A-Za-z0-9_]', '_', sys.argv[1]).lower().strip('_')
print(s[:55])
PY
}

connector_name() {
  local table="$1"
  ident "${PREFIX}-${table}" | tr '_' '-'
}

connector_name_prefix() {
  ident "${PREFIX}" | tr '_' '-'
}

slot_name() {
  ident "${PREFIX}_slot"
}

publication_name() {
  ident "${PREFIX}_pub"
}

quote_ident() {
  python3 - "$1" <<'PY'
import sys
print('"' + sys.argv[1].replace('"', '""') + '"')
PY
}

pg_exec() {
  kubectl -n "${NS}" exec chbench-postgres-0 -- psql -U "${PG_USER}" -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"
}

rpk() {
  kubectl -n "${NS}" exec "${REDPANDA_POD}" -- rpk "$@" --brokers 127.0.0.1:9092
}

wait_connect() {
  kubectl -n "${NS}" port-forward "${CONNECT_SERVICE}" "${CONNECT_LOCAL_PORT}:8083" >"${pf_log}" 2>&1 &
  pf_pid=$!
  for _ in {1..120}; do
    if curl --max-time 10 -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${pf_pid}" 2>/dev/null; then
      echo "kubectl port-forward exited early:" >&2
      cat "${pf_log}" >&2 || true
      exit 1
    fi
    sleep 1
  done
  echo "Timed out waiting for Debezium Connect at ${CONNECT_URL}" >&2
  cat "${pf_log}" >&2 || true
  exit 1
}

connector_state_json() {
  curl --max-time 30 -fsS "${CONNECT_URL}/connectors/$1/status"
}

topic_offsets() {
  local topic="$1"
  rpk topic describe "${topic}" --print-partitions 2>/dev/null \
    | awk 'NR>1 {sum += $6} END {print sum+0}'
}

source_count() {
  local table="$1"
  pg_exec "${PG_DB}" "select count(*) from public.${table};" | tr -d '[:space:]'
}

wait_connect

echo "CH-benCH Debezium preload: namespace=${NS} prefix=${PREFIX} postgres=${PG_HOST}:${PG_PORT}/${PG_DB} template=${PG_TEMPLATE_DB}"

# Stop old connectors for this prefix before dropping slots/topics. Connector
# names include a timestamp, so enumerate by prefix. Also delete stale per-table
# connector names from older attempts.
name_prefix="$(connector_name_prefix)"
connector_names="$(curl --max-time 30 -fsS "${CONNECT_URL}/connectors" | jq -r --arg p "${name_prefix}" '.[] | select(startswith($p))')"
for name in ${connector_names} "${name_prefix}" $(for table in "${TABLES[@]}"; do connector_name "${table}"; done); do
  [ -n "${name}" ] || continue
  code="$(curl --max-time 30 -sS -o /tmp/chbench-delete-connector-body -w '%{http_code}' -X DELETE "${CONNECT_URL}/connectors/${name}" || true)"
  if [[ "${code}" != "200" && "${code}" != "202" && "${code}" != "204" && "${code}" != "404" && "${code}" != "000" ]]; then
    echo "warning: deleting connector ${name} returned ${code}: $(cat /tmp/chbench-delete-connector-body 2>/dev/null || true)" >&2
  fi
done
sleep 5

# Restore an isolated source DB from the cached SF template. This avoids races
# with unrelated benchmark jobs that drop/restore the shared `chbench` DB.
pg_db_ident="$(quote_ident "${PG_DB}")"
pg_template_ident="$(quote_ident "${PG_TEMPLATE_DB}")"
cleanup_sql="DO \$\$ DECLARE r RECORD; BEGIN
  FOR r IN SELECT slot_name, active_pid FROM pg_replication_slots WHERE database = '${PG_DB}' OR slot_name LIKE '$(ident "${PREFIX}")%' LOOP
    IF r.active_pid IS NOT NULL THEN
      PERFORM pg_terminate_backend(r.active_pid);
    END IF;
  END LOOP;
  FOR r IN SELECT slot_name FROM pg_replication_slots WHERE database = '${PG_DB}' OR slot_name LIKE '$(ident "${PREFIX}")%' LOOP
    PERFORM pg_drop_replication_slot(r.slot_name);
  END LOOP;
END \$\$;"
pg_exec postgres "${cleanup_sql}" >/dev/null
pg_exec postgres "DROP DATABASE IF EXISTS ${pg_db_ident} WITH (FORCE);" >/dev/null
pg_exec postgres "CREATE DATABASE ${pg_db_ident} TEMPLATE ${pg_template_ident} OWNER ${PG_USER} STRATEGY=FILE_COPY;" >/dev/null
pg_exec postgres "ALTER DATABASE ${pg_db_ident} WITH ALLOW_CONNECTIONS true;" >/dev/null
echo "restored isolated source database ${PG_DB} from ${PG_TEMPLATE_DB}"

# Remove old data topics for this prefix. Only delete topics that exist; some rpk
# versions can wait longer than expected on deletes for absent topics.
existing_topics="$(rpk topic list | awk 'NR>1 {print $1}')"
for table in "${TABLES[@]}"; do
  topic="${PREFIX}.public.${table}"
  if printf '%s\n' "${existing_topics}" | grep -Fxq "${topic}"; then
    rpk topic delete "${topic}" >/dev/null 2>&1 || true
  fi
done

# Source row counts are the completion target for snapshot topics.
for table in "${TABLES[@]}"; do
  source_count "${table}" >"${tmpdir}/count-${table}"
  echo "source ${table}: $(cat "${tmpdir}/count-${table}") rows"
done

# Register one connector containing all CDC tables. A single connector is required
# when using one shared Debezium topic prefix because Kafka Connect offsets are keyed
# by that logical server name. It snapshots serially, but preload is outside the timed run.
name="$(connector_name_prefix)-$(date +%s)"
slot="$(slot_name)"
pub="$(publication_name)"
include_list="$(printf 'public.%s,' "${TABLES[@]}")"
include_list="${include_list%,}"
body="${tmpdir}/${name}.json"
cat >"${body}" <<JSON
{
  "name": "${name}",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "${PG_HOST}",
    "database.port": "${PG_PORT}",
    "database.user": "${PG_USER}",
    "database.password": "${PG_PASS}",
    "database.dbname": "${PG_DB}",
    "topic.prefix": "${PREFIX}",
    "table.include.list": "${include_list}",
    "plugin.name": "pgoutput",
    "slot.name": "${slot}",
    "slot.drop.on.stop": "true",
    "publication.name": "${pub}",
    "publication.autocreate.mode": "filtered",
    "snapshot.mode": "initial",
    "snapshot.fetch.size": "50000",
    "max.batch.size": "50000",
    "max.queue.size": "200000",
    "poll.interval.ms": "100",
    "tasks.max": "1",
    "heartbeat.interval.ms": "10000",
    "include.schema.changes": "false",
    "tombstones.on.delete": "false",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
JSON
curl --max-time 60 -fsS -H 'Content-Type: application/json' -X POST --data @"${body}" "${CONNECT_URL}/connectors" >/dev/null
echo "registered ${name} -> ${PREFIX}.public.{${TABLES[*]}}"

# Wait for the connector task to be RUNNING.
for _ in {1..300}; do
  status="$(connector_state_json "${name}" || true)"
  if echo "${status}" | jq -e '.tasks | length > 0 and all(.state == "RUNNING")' >/dev/null 2>&1; then
    echo "connector ${name}: RUNNING"
    break
  fi
  if echo "${status}" | jq -e '.connector.state == "FAILED" or any(.tasks[]?; .state == "FAILED")' >/dev/null 2>&1; then
    echo "connector ${name} failed: ${status}" >&2
    exit 1
  fi
  sleep 2
done

# Wait until each Kafka topic has at least the source table's snapshot row count.
start=$(date +%s)
while true; do
  pending=()
  for table in "${TABLES[@]}"; do
    topic="${PREFIX}.public.${table}"
    have="$(topic_offsets "${topic}")"
    want="$(cat "${tmpdir}/count-${table}")"
    if (( have < want )); then
      pending+=("${table}:${have}/${want}")
    fi
  done
  if (( ${#pending[@]} == 0 )); then
    elapsed=$(( $(date +%s) - start ))
    echo "CH-benCH Debezium preload complete in ${elapsed}s for prefix ${PREFIX}"
    rpk topic list | grep "^${PREFIX}\.public\." || true
    exit 0
  fi
  elapsed=$(( $(date +%s) - start ))
  if (( elapsed > ${CHBENCH_DEBEZIUM_PRELOAD_TIMEOUT:-7200} )); then
    echo "Timed out waiting for Debezium preload; pending: ${pending[*]}" >&2
    exit 1
  fi
  if (( elapsed % 30 == 0 )); then
    echo "preload still catching up (${elapsed}s): ${pending[*]}"
  fi
  sleep 5
done
