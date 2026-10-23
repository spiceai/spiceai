#!/usr/bin/env bash
# Drive a real spiced file-connector spicepod over HTTP SQL.
# Subcommands: seed | launch | doctor | drive | cleanup | compare
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
verify-file-connector.sh <seed|launch|doctor|drive|cleanup|compare> [flags]

  seed     --work DIR
  launch   --spiced PATH --work DIR [--http ADDR] [--flight ADDR] [--metrics ADDR]
  doctor   --work DIR --http ADDR [--spiced PATH]
  drive    --work DIR --http ADDR --evidence DIR --label LABEL
  cleanup  --work DIR
  compare  --trunk-spiced PATH --branch-spiced PATH --evidence DIR
EOF
}

die() { echo "error: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

WORK=""
SPICED=""
HTTP_ADDR=""
FLIGHT_ADDR=""
METRICS_ADDR=""
EVIDENCE=""
LABEL=""
TRUNK_SPICED=""
BRANCH_SPICED=""

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --work) WORK="$2"; shift 2 ;;
      --spiced) SPICED="$2"; shift 2 ;;
      --http) HTTP_ADDR="$2"; shift 2 ;;
      --flight) FLIGHT_ADDR="$2"; shift 2 ;;
      --metrics) METRICS_ADDR="$2"; shift 2 ;;
      --evidence) EVIDENCE="$2"; shift 2 ;;
      --label) LABEL="$2"; shift 2 ;;
      --trunk-spiced) TRUNK_SPICED="$2"; shift 2 ;;
      --branch-spiced) BRANCH_SPICED="$2"; shift 2 ;;
      --help|-h) usage; exit 0 ;;
      *) die "unknown flag: $1" ;;
    esac
  done
}

pidfile() { echo "${WORK}/spiced.pid"; }
logfile() { echo "${WORK}/spiced.log"; }
binfile() { echo "${WORK}/spiced.binary"; }
identityfile() { echo "${WORK}/spiced.identity"; }

# /proc/<pid>/stat fields after comm. comm can contain spaces and parens,
# so split on the last `)`. Index 0 is state; 19 is starttime (field 22).
# Empty stdout when the file is unreadable — never treat empty as a value.
process_stat_field() {
  local pid="$1"
  local index="$2"
  python3 - "$pid" "$index" <<'PY'
import pathlib, sys
pid = sys.argv[1]
index = int(sys.argv[2])
try:
    text = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
except OSError:
    sys.exit(0)
parts = text.rsplit(")", 1)
if len(parts) != 2:
    sys.exit(0)
fields = parts[1].split()
if index < 0 or index >= len(fields):
    sys.exit(0)
print(fields[index])
PY
}

process_starttime() {
  local start
  start="$(process_stat_field "$1" 19)"
  [[ -n "$start" ]] || return 1
  printf '%s\n' "$start"
}

process_state() {
  process_stat_field "$1" 0
}

# Gone, or an unreaped zombie. `kill -0` succeeds for state Z, but
# `/proc/<pid>/exe` is gone, so an identity check would look like a
# recycled PID. An unreadable state is not proof of termination.
process_is_stopped() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null || return 0
  local state
  state="$(process_state "$pid")"
  [[ "$state" == "Z" ]]
}

write_process_identity() {
  local pid="$1"
  local exe="$2"
  local start
  start="$(process_starttime "$pid")"
  printf '%s\t%s\t%s\n' "$pid" "$exe" "$start" >"$(identityfile)"
}

# Same pid, same executable, same starttime as launch recorded.
process_matches_identity() {
  local pid="$1"
  local want_exe="$2"
  local want_start="$3"
  kill -0 "$pid" 2>/dev/null || return 1
  local exe start
  exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
  [[ -n "$exe" && "$exe" == "$want_exe" ]] || return 1
  start="$(process_starttime "$pid" 2>/dev/null || true)"
  [[ -n "$start" && "$start" == "$want_start" ]]
}

# True when `$WORK` still owns a live process: pidfile + identity match
# pid, `/proc/<pid>/exe`, and starttime. A dead or recycled PID is not tracked.
work_tracks_live_spiced() {
  local pf identf
  pf="$(pidfile)"
  identf="$(identityfile)"
  [[ -f "$pf" && -f "$identf" ]] || return 1
  local pid want_pid want_exe want_start
  pid="$(cat "$pf")"
  IFS=$'\t' read -r want_pid want_exe want_start <"$identf"
  [[ -n "$pid" && "$pid" == "$want_pid" ]] || return 1
  process_matches_identity "$pid" "$want_exe" "$want_start"
}

# Refuse a second launch before truncating the log or replacing ownership
# records. Stale/dead/recycled PID files may relaunch.
refuse_if_already_running() {
  work_tracks_live_spiced || return 0
  local pid
  pid="$(cat "$(pidfile)")"
  die "work directory already tracks live pid ${pid}; refusing to launch a second instance"
}

# Resolve `--work` after creating it. `cmd_launch` then cds to `$WORK`; a
# relative path such as `work` would make `logfile` expand to `work/spiced.log`
# inside that directory (`work/work/spiced.log`).
require_work() {
  [[ -n "$WORK" ]] || die "--work is required"
  mkdir -p "$WORK"
  WORK="$(cd "$WORK" && pwd)"
}

# Resolve `--spiced` in the caller's directory. `cmd_launch` then cds to
# `$WORK`; a relative path such as `target/debug/spiced` would miss there.
resolve_spiced() {
  local path="$1"
  [[ -n "$path" ]] || die "spiced path is empty"
  [[ -x "$path" ]] || die "spiced is not executable: $path"
  local resolved
  resolved="$(readlink -f -- "$path" 2>/dev/null || true)"
  if [[ -z "$resolved" ]]; then
    if [[ "$path" == /* ]]; then
      resolved="$path"
    else
      resolved="$(cd "$(dirname -- "$path")" && pwd)/$(basename -- "$path")"
    fi
  fi
  [[ -x "$resolved" ]] || die "spiced is not executable: $resolved"
  printf '%s\n' "$resolved"
}

write_events_csv() {
  local path="$1"
  local header="$2"
  shift 2
  {
    printf '%s\n' "$header"
    printf '%s\n' "$@"
  } >"$path"
}

write_events_jsonl() {
  local path="$1"
  shift
  printf '%s\n' "$@" >"$path"
}

write_events_json_array() {
  local path="$1"
  shift
  {
    echo '['
    local i=0
    local n=$#
    for row in "$@"; do
      i=$((i + 1))
      if (( i < n )); then
        printf '  %s,\n' "$row"
      else
        printf '  %s\n' "$row"
      fi
    done
    echo ']'
  } >"$path"
}

write_events_json_pointer() {
  local path="$1"
  shift
  {
    echo '{"data":['
    local i=0
    local n=$#
    for row in "$@"; do
      i=$((i + 1))
      if (( i < n )); then
        printf '  %s,\n' "$row"
      else
        printf '  %s\n' "$row"
      fi
    done
    echo ']}'
  } >"$path"
}

JSON_BASELINE_1='{"event_id":1,"user_id":10,"region":"us","amount_cents":100,"ts":"t0"}'
JSON_BASELINE_2='{"event_id":2,"user_id":20,"region":"eu","amount_cents":200,"ts":"t1"}'
JSON_REORDER_1='{"event_id":1,"amount_cents":100,"region":"us","user_id":10,"ts":"t0"}'
JSON_REORDER_2='{"event_id":2,"amount_cents":200,"region":"eu","user_id":20,"ts":"t1"}'
JSON_RENAME_1='{"event_id":1,"user_id":10,"region":"us","amt":100,"ts":"t0"}'
JSON_RENAME_2='{"event_id":2,"user_id":20,"region":"eu","amt":200,"ts":"t1"}'

ALL_DATASETS=(csv_fed csv_acc json_fed json_acc json_array_fed json_array_acc json_ptr_fed json_ptr_acc)
FED_DATASETS=(csv_fed json_fed json_array_fed json_ptr_fed)
ACC_DATASETS=(csv_acc json_acc json_array_acc json_ptr_acc)

write_all_json_shapes() {
  local one="$1"
  local two="$2"
  write_events_jsonl "${WORK}/data/events.jsonl" "$one" "$two"
  write_events_json_array "${WORK}/data/events.json" "$one" "$two"
  write_events_json_pointer "${WORK}/data/events_pointer.json" "$one" "$two"
}

write_all_json_empty() {
  # Blank-line JSONL, `[]`, and `{"data":[]}` — zero records, not missing keys.
  printf '\n\n' >"${WORK}/data/events.jsonl"
  write_events_json_array "${WORK}/data/events.json"
  write_events_json_pointer "${WORK}/data/events_pointer.json"
}

seed_files() {
  local data="${WORK}/data"
  mkdir -p "$data"
  write_events_csv "${data}/events.csv" \
    "event_id,user_id,region,amount_cents,ts" \
    "1,10,us,100,t0" \
    "2,20,eu,200,t1"
  write_all_json_shapes "$JSON_BASELINE_1" "$JSON_BASELINE_2"
}

seed_spicepod() {
  mkdir -p "${WORK}/data"
  local data
  data="$(cd "${WORK}/data" && pwd)"
  cat >"${WORK}/spicepod.yaml" <<YAML
version: v1
kind: Spicepod
name: file-connector-verify
datasets:
  - from: file:${data}/events.csv
    name: csv_fed
    params:
      file_format: csv
  - from: file:${data}/events.csv
    name: csv_acc
    params:
      file_format: csv
    acceleration:
      enabled: true
      engine: arrow
      refresh_mode: full
  - from: file:${data}/events.jsonl
    name: json_fed
    params:
      file_format: jsonl
  - from: file:${data}/events.jsonl
    name: json_acc
    params:
      file_format: jsonl
    acceleration:
      enabled: true
      engine: arrow
      refresh_mode: full
  - from: file:${data}/events.json
    name: json_array_fed
    params:
      file_format: json
  - from: file:${data}/events.json
    name: json_array_acc
    params:
      file_format: json
    acceleration:
      enabled: true
      engine: arrow
      refresh_mode: full
  - from: file:${data}/events_pointer.json
    name: json_ptr_fed
    params:
      file_format: json
      json_pointer: /data
  - from: file:${data}/events_pointer.json
    name: json_ptr_acc
    params:
      file_format: json
      json_pointer: /data
    acceleration:
      enabled: true
      engine: arrow
      refresh_mode: full
YAML
  seed_files
}

cmd_seed() {
  require_work
  seed_spicepod
  echo "seeded ${WORK}/spicepod.yaml"
}

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_ready() {
  local url="http://${HTTP_ADDR}/v1/ready"
  local deadline=$((SECONDS + 90))
  local last="unreachable"
  : >"${WORK}/ready.body"
  while (( SECONDS < deadline )); do
    if [[ -f "$(pidfile)" ]]; then
      local pid
      pid="$(cat "$(pidfile)")"
      if ! kill -0 "$pid" 2>/dev/null; then
        die "spiced pid ${pid} exited before ready; see $(logfile)"
      fi
    fi
    local code body
    code="$(curl -sS -o "${WORK}/ready.body" -w '%{http_code}' --max-time 2 "$url" || echo 000)"
    body="$(tr -d '\r' <"${WORK}/ready.body" 2>/dev/null || true)"
    last="${code} ${body}"
    if [[ "$code" == "200" && "$body" == "ready" ]]; then
      echo "ready: ${url} -> 200 ready"
      return 0
    fi
    sleep 0.25
  done
  die "GET ${url} did not become 200 ready within 90s (last: ${last}); see $(logfile)"
}

cmd_launch() {
  [[ -n "$WORK" && -n "$SPICED" ]] || die "--work and --spiced are required"
  require_cmd curl
  require_work
  SPICED="$(resolve_spiced "$SPICED")"
  refuse_if_already_running
  [[ -f "${WORK}/spicepod.yaml" ]] || seed_spicepod
  HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:$(free_port)}"
  FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:$(free_port)}"
  METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:$(free_port)}"
  : >"$(logfile)"
  # Resolve `file:` paths and the spicepod from the work directory.
  local pid
  pid="$(
    cd "$WORK" || exit 1
    nohup "$SPICED" --telemetry-enabled=false \
      --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" \
      . >"$(logfile)" 2>&1 &
    echo $!
  )"
  echo "$pid" >"$(pidfile)"
  printf '%s\n' "$SPICED" >"$(binfile)"
  write_process_identity "$pid" "$SPICED"
  printf '%s\n' "$HTTP_ADDR" >"${WORK}/http.addr"
  printf '%s\n' "$FLIGHT_ADDR" >"${WORK}/flight.addr"
  echo "started pid=${pid} http=${HTTP_ADDR} flight=${FLIGHT_ADDR} log=$(logfile)"
  wait_ready
}

cmd_doctor() {
  [[ -n "$HTTP_ADDR" ]] || die "--http is required"
  require_work
  local pf identf
  pf="$(pidfile)"
  identf="$(identityfile)"
  [[ -f "$pf" ]] || die "no pidfile at $pf"
  [[ -f "$identf" ]] || die "no process identity at ${identf}; refusing to accept pid $(cat "$pf")"
  local pid
  pid="$(cat "$pf")"
  local want_pid want_exe want_start
  IFS=$'\t' read -r want_pid want_exe want_start <"$identf"
  [[ "$pid" == "$want_pid" ]] || die "pidfile ${pid} does not match identity ${want_pid}"
  if ! process_matches_identity "$pid" "$want_exe" "$want_start"; then
    die "pid ${pid} is not the launched spiced (exe/starttime mismatch); refusing to accept"
  fi
  local exe
  exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
  if [[ -n "$SPICED" ]]; then
    local want
    want="$(readlink -f "$SPICED")"
    [[ "$want" == "$want_exe" && "$exe" == "$want" ]] || \
      die "pid $pid exe is ${exe}, expected ${want} (launch identity ${want_exe})"
  fi
  local code body
  code="$(curl -sS -o "${WORK}/ready.body" -w '%{http_code}' --max-time 5 "http://${HTTP_ADDR}/v1/ready" || echo 000)"
  body="$(tr -d '\r' <"${WORK}/ready.body" 2>/dev/null || true)"
  echo "doctor pid=${pid} exe=${exe} ready=${code} ${body}"
  [[ "$code" != "000" ]] || die "/v1/ready is unreachable at ${HTTP_ADDR}"
  local datasets
  datasets="$(curl -sS --max-time 10 "http://${HTTP_ADDR}/v1/datasets?status=true")"
  python3 - "$datasets" <<'PY'
import json, sys
needed = {"csv_fed", "csv_acc", "json_fed", "json_acc", "json_array_fed", "json_array_acc", "json_ptr_fed", "json_ptr_acc"}
data = json.loads(sys.argv[1])
names = {d.get("name") for d in data}
missing = needed - names
if missing:
    raise SystemExit(f"datasets missing {sorted(missing)}: {data}")
print("doctor datasets:", ", ".join(sorted(names)))
for d in data:
    if d.get("name") in needed:
        print(f"  {d.get('name')}: status={d.get('status')} error_message={d.get('error_message')}")
PY
}

sql_query() {
  local sql="$1"
  local out="$2"
  mkdir -p "$(dirname "$out")"
  {
    echo "SQL: ${sql}"
    echo "---"
    curl -sS -D - --max-time 30 \
      -H "Content-Type: text/plain" \
      -H "Accept: application/json" \
      --data-binary "$sql" \
      "http://${HTTP_ADDR}/v1/sql" || true
  } >"$out"
}

extract_sums() {
  python3 - "$1" <<'PY'
import json, sys
text = open(sys.argv[1], encoding="utf-8").read()
body = text.split("---", 1)[-1]
# drop HTTP headers
idx = body.find("[")
obj = body.find("{")
if idx < 0 and obj < 0:
    print("parse_error")
    raise SystemExit(0)
start = idx if idx >= 0 and (obj < 0 or idx < obj) else obj
payload = body[start:].strip()
try:
    data = json.loads(payload)
except json.JSONDecodeError:
    print("parse_error")
    raise SystemExit(0)
row = data[0] if isinstance(data, list) and data else data
if not isinstance(row, dict):
    print("parse_error")
    raise SystemExit(0)
def val(key):
    for k, v in row.items():
        if k == key or k.endswith("." + key) or k.lower().endswith(key):
            if v is None:
                return "null"
            return str(v)
    return "missing"
print(f"{val('amount_cents')},{val('user_id')}")
PY
}

datasets_status() {
  local out="$1"
  curl -sS --max-time 10 "http://${HTTP_ADDR}/v1/datasets?status=true" >"$out"
}

ready_capture() {
  local out="$1"
  local code body
  code="$(curl -sS -o "${WORK}/ready.body" -w '%{http_code}' --max-time 5 "http://${HTTP_ADDR}/v1/ready" || echo 000)"
  body="$(tr -d '\r' <"${WORK}/ready.body" 2>/dev/null || true)"
  printf '%s %s\n' "$code" "$body" >"$out"
}

refresh_dataset() {
  local name="$1"
  local out="$2"
  local code
  code="$(curl -sS -o "${out}.body" -w '%{http_code}' --max-time 15 \
    -X POST "http://${HTTP_ADDR}/v1/datasets/${name}/acceleration/refresh" || echo 000)"
  echo "$code" >"${out}.status"
  cat "${out}.body" >"${out}.body.txt" 2>/dev/null || true
  echo "refresh ${name} -> HTTP ${code} $(tr -d '\n' <"${out}.body" | head -c 200)"
}

dataset_refresh_fields() {
  local name="$1"
  local json
  json="$(curl -sS --max-time 10 "http://${HTTP_ADDR}/v1/datasets?status=true")"
  python3 - "$json" "$name" <<'PY'
import json, sys
data = json.loads(sys.argv[1])
name = sys.argv[2]
for d in data:
    if d.get("name") == name:
        print(f"{d.get('status') or ''}\t{d.get('last_refresh') or ''}\t{d.get('error_message') or ''}")
        raise SystemExit(0)
print("missing\t\t")
PY
}

refresh_task_count() {
  local name="$1"
  [[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "invalid dataset name: $name"
  local payload
  payload="$(curl -sS --max-time 15 \
    -H "Content-Type: text/plain" \
    -H "Accept: application/json" \
    --data-binary "SELECT count(*) AS n FROM runtime.task_history WHERE task = 'acceleration_refresh' AND \"input\" = '${name}'" \
    "http://${HTTP_ADDR}/v1/sql" || true)"
  python3 - "$payload" <<'PY'
import json, sys
text = sys.argv[1]
idx = text.find("[")
if idx < 0:
    print("")
    raise SystemExit(0)
try:
    data = json.loads(text[idx:])
except json.JSONDecodeError:
    print("")
    raise SystemExit(0)
row = data[0] if isinstance(data, list) and data else data
if not isinstance(row, dict):
    print("")
    raise SystemExit(0)
for key, value in row.items():
    if key == "n" or key.endswith(".n"):
        print(int(value))
        raise SystemExit(0)
print("")
PY
}

refresh_completion_marker() {
  local name="$1"
  printf '%s\t%s\n' "$(dataset_refresh_fields "$name")" "$(refresh_task_count "$name")"
}

# A new completion is a terminal status plus a marker that moved: a new
# `acceleration_refresh` task-history row, a new `last_refresh` (success), or a
# new `error_message` (failure does not advance `last_refresh`).
refresh_is_complete() {
  python3 - "$1" "$2" <<'PY'
import sys

def split(raw: str) -> tuple[str, str, str, str]:
    parts = raw.split("\t")
    while len(parts) < 4:
        parts.append("")
    return parts[0], parts[1], parts[2], parts[3]

_, b_last, b_err, b_n = split(sys.argv[1])
n_status, n_last, n_err, n_n = split(sys.argv[2])
terminal = n_status not in {"Refreshing", "Initializing", "missing", ""}
try:
    new_task = bool(b_n) and bool(n_n) and int(n_n) > int(b_n)
except ValueError:
    new_task = False
last_moved = bool(n_last) and n_last != b_last
err_moved = n_err != b_err
print("yes" if terminal and (new_task or last_moved or err_moved) else "no")
PY
}

wait_refresh_settled() {
  local name="$1"
  local baseline="$2"
  local deadline=$((SECONDS + 60))
  local now=""
  while (( SECONDS < deadline )); do
    now="$(refresh_completion_marker "$name")"
    if [[ "$(refresh_is_complete "$baseline" "$now")" == "yes" ]]; then
      echo "refresh settled ${name} status=${now%%$'\t'*}"
      return 0
    fi
    sleep 0.25
  done
  die "dataset ${name} refresh did not complete within 60s (last: ${now:-none})"
}

refresh_all_and_wait() {
  local step="$1"
  local ev="$2"
  local ds
  local baselines=()
  for ds in "${ACC_DATASETS[@]}"; do
    baselines+=("$(refresh_completion_marker "$ds")")
    refresh_dataset "$ds" "${ev}/refresh-${step}-${ds}"
  done
  local i=0
  for ds in "${ACC_DATASETS[@]}"; do
    wait_refresh_settled "$ds" "${baselines[$i]}"
    i=$((i + 1))
  done
}

summarize_datasets() {
  python3 - "$1" <<'PY'
import json, sys
data = json.loads(open(sys.argv[1], encoding="utf-8").read())
parts = []
for name in ("csv_fed", "csv_acc", "json_fed", "json_acc", "json_array_fed", "json_array_acc", "json_ptr_fed", "json_ptr_acc"):
    row = next((d for d in data if d.get("name") == name), None)
    if row is None:
        parts.append(f"{name}=absent")
        continue
    err = row.get("error_message") or ""
    err = err.replace("\n", " ")
    parts.append(f"{name}={row.get('status')}" + (f" error={err}" if err else ""))
print(" | ".join(parts))
PY
}

capture_log_slice() {
  local log="$1"
  local start_bytes="$2"
  local out="$3"
  if [[ ! -f "$log" ]]; then
    : >"$out"
    return
  fi
  tail -c +"$((start_bytes + 1))" "$log" | grep -E "WARN|ERROR|amount_cents|registered column|Dataset '" >"$out" || true
}

query_dataset() {
  local step="$1"
  local dataset="$2"
  local ev="$3"
  local sql="SELECT sum(amount_cents) AS amount_cents, sum(user_id) AS user_id FROM ${dataset}"
  local out="${ev}/sql-${step}-${dataset}.json"
  sql_query "$sql" "$out"
  local sums
  sums="$(extract_sums "$out")"
  echo "${step} ${dataset} sums=${sums}"
  echo "${step} ${dataset} ${sums}" >>"${ev}/summary.txt"
}

cmd_drive() {
  [[ -n "$HTTP_ADDR" && -n "$EVIDENCE" && -n "$LABEL" ]] || \
    die "--work --http --evidence --label are required"
  require_work
  local ev="${EVIDENCE}/${LABEL}"
  mkdir -p "$ev"
  : >"${ev}/summary.txt"
  cp "${WORK}/spicepod.yaml" "${ev}/spicepod.yaml"
  echo "label=${LABEL}" >>"${ev}/summary.txt"
  echo "http=${HTTP_ADDR}" >>"${ev}/summary.txt"
  if [[ -x "$SPICED" ]]; then
    "$SPICED" --version >>"${ev}/summary.txt" 2>&1 || true
  elif [[ -f "$(binfile)" ]]; then
    local bin
    bin="$(cat "$(binfile)")"
    [[ -x "$bin" ]] && "$bin" --version >>"${ev}/summary.txt" 2>&1 || true
  fi
  date -u +'utc=%Y-%m-%dT%H:%M:%SZ' >>"${ev}/summary.txt"

  seed_files
  cp "${WORK}/data/events.csv" "${ev}/events.baseline.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.baseline.jsonl"
  cp "${WORK}/data/events.json" "${ev}/events.baseline.json"
  cp "${WORK}/data/events_pointer.json" "${ev}/events.baseline.pointer.json"

  ready_capture "${ev}/ready-baseline.txt"
  datasets_status "${ev}/datasets-baseline.json"
  echo "baseline datasets: $(summarize_datasets "${ev}/datasets-baseline.json")" | tee -a "${ev}/summary.txt"
  local ds
  for ds in "${ALL_DATASETS[@]}"; do
    query_dataset baseline "$ds" "$ev"
  done

  write_events_csv "${WORK}/data/events.csv" \
    "event_id,amount_cents,region,user_id,ts" \
    "1,100,us,10,t0" \
    "2,200,eu,20,t1"
  write_all_json_shapes "$JSON_REORDER_1" "$JSON_REORDER_2"
  cp "${WORK}/data/events.csv" "${ev}/events.reordered.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.reordered.jsonl"
  cp "${WORK}/data/events.json" "${ev}/events.reordered.json"
  cp "${WORK}/data/events_pointer.json" "${ev}/events.reordered.pointer.json"

  for ds in "${FED_DATASETS[@]}"; do
    query_dataset reorder-fed "$ds" "$ev"
  done
  refresh_all_and_wait reorder "$ev"
  for ds in "${ACC_DATASETS[@]}"; do
    query_dataset reorder-acc "$ds" "$ev"
  done
  datasets_status "${ev}/datasets-reorder.json"
  ready_capture "${ev}/ready-reorder.txt"
  echo "reorder datasets: $(summarize_datasets "${ev}/datasets-reorder.json")" | tee -a "${ev}/summary.txt"

  local log_at
  log_at="$(wc -c <"$(logfile)" | tr -d ' ')"
  write_events_csv "${WORK}/data/events.csv" \
    "event_id,user_id,region,amt,ts" \
    "1,10,us,100,t0" \
    "2,20,eu,200,t1"
  write_all_json_shapes "$JSON_RENAME_1" "$JSON_RENAME_2"
  cp "${WORK}/data/events.csv" "${ev}/events.renamed.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.renamed.jsonl"
  cp "${WORK}/data/events.json" "${ev}/events.renamed.json"
  cp "${WORK}/data/events_pointer.json" "${ev}/events.renamed.pointer.json"

  for ds in "${FED_DATASETS[@]}"; do
    query_dataset rename-fed "$ds" "$ev"
  done
  capture_log_slice "$(logfile)" "$log_at" "${ev}/log-rename-fed.txt"
  refresh_all_and_wait rename "$ev"
  for ds in "${ACC_DATASETS[@]}"; do
    query_dataset rename-acc "$ds" "$ev"
  done
  datasets_status "${ev}/datasets-rename.json"
  ready_capture "${ev}/ready-rename.txt"
  capture_log_slice "$(logfile)" "$log_at" "${ev}/log-rename.txt"
  echo "rename datasets: $(summarize_datasets "${ev}/datasets-rename.json")" | tee -a "${ev}/summary.txt"

  write_events_csv "${WORK}/data/events.csv" \
    "event_id,user_id,region,amount_cents,ts" \
    "1,10,us,100,t0" \
    "2,20,eu,200,t1"
  write_all_json_shapes "$JSON_BASELINE_1" "$JSON_BASELINE_2"
  cp "${WORK}/data/events.csv" "${ev}/events.restored.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.restored.jsonl"
  cp "${WORK}/data/events.json" "${ev}/events.restored.json"
  cp "${WORK}/data/events_pointer.json" "${ev}/events.restored.pointer.json"

  for ds in "${FED_DATASETS[@]}"; do
    query_dataset restore-fed "$ds" "$ev"
  done
  refresh_all_and_wait restore "$ev"
  for ds in "${ACC_DATASETS[@]}"; do
    query_dataset restore-acc "$ds" "$ev"
  done
  datasets_status "${ev}/datasets-restore.json"
  ready_capture "${ev}/ready-restore.txt"
  echo "restore datasets: $(summarize_datasets "${ev}/datasets-restore.json")" | tee -a "${ev}/summary.txt"

  write_all_json_empty
  cp "${WORK}/data/events.jsonl" "${ev}/events.empty.jsonl"
  cp "${WORK}/data/events.json" "${ev}/events.empty.json"
  cp "${WORK}/data/events_pointer.json" "${ev}/events.empty.pointer.json"

  for ds in json_fed json_array_fed json_ptr_fed; do
    query_dataset empty-fed "$ds" "$ev"
  done
  refresh_all_and_wait empty "$ev"
  for ds in json_acc json_array_acc json_ptr_acc; do
    query_dataset empty-acc "$ds" "$ev"
  done
  query_dataset empty-acc csv_acc "$ev"
  datasets_status "${ev}/datasets-empty.json"
  ready_capture "${ev}/ready-empty.txt"
  echo "empty datasets: $(summarize_datasets "${ev}/datasets-empty.json")" | tee -a "${ev}/summary.txt"

  cp "$(logfile)" "${ev}/spiced.log"
  echo "drive complete ${ev}"
}

cmd_cleanup() {
  require_work
  local pf identf
  pf="$(pidfile)"
  identf="$(identityfile)"
  if [[ -f "$pf" ]]; then
    local pid
    pid="$(cat "$pf")"
    if process_is_stopped "$pid"; then
      rm -f "$pf" "$identf"
      echo "pid ${pid} is already gone"
    else
      [[ -f "$identf" ]] || die "no process identity at ${identf}; refusing to signal pid ${pid}"
      local want_pid want_exe want_start
      IFS=$'\t' read -r want_pid want_exe want_start <"$identf"
      [[ "$pid" == "$want_pid" ]] || die "pidfile ${pid} does not match identity ${want_pid}; refusing to signal"
      local bin
      bin="$(cat "$(binfile)" 2>/dev/null || true)"
      [[ -n "$bin" && "$bin" == "$want_exe" ]] || die "identity exe ${want_exe} does not match $(binfile); refusing to signal pid ${pid}"
      if ! process_matches_identity "$pid" "$want_exe" "$want_start"; then
        die "pid ${pid} is not the launched spiced (exe/starttime mismatch); refusing to signal"
      fi
      kill -TERM "$pid" 2>/dev/null || true
      local i
      for i in $(seq 1 40); do
        process_is_stopped "$pid" && break
        sleep 0.1
      done
      if ! process_is_stopped "$pid"; then
        if ! process_matches_identity "$pid" "$want_exe" "$want_start"; then
          # Already sent TERM. A dying process can lose `/proc/<pid>/exe`
          # before state is `Z` or `kill -0` fails. Re-check before treating
          # that as a recycled PID.
          process_is_stopped "$pid" || \
            die "pid ${pid} changed identity before KILL; refusing to signal"
        else
          kill -KILL "$pid" 2>/dev/null || true
        fi
      fi
      rm -f "$pf" "$identf"
      echo "stopped pid=${pid}"
    fi
  else
    echo "no pidfile; nothing to stop"
    rm -f "$identf"
  fi
  rm -rf "${WORK}/.spice"
}

write_compare_report() {
  local ev="$1"
  python3 - "$ev" <<'PY'
import pathlib, sys
ev = pathlib.Path(sys.argv[1])
report = ev / "COMPARE.md"

def sums(label, step, dataset):
    summary = (ev / label / "summary.txt").read_text(encoding="utf-8")
    key = f"{step} {dataset} "
    for line in summary.splitlines():
        if line.startswith(key) and "," in line.split()[-1]:
            return line.split()[-1]
    return "missing"

def datasets(label, step):
    p = ev / label / f"datasets-{step}.json"
    if not p.exists():
        return "missing"
    import json
    data = json.loads(p.read_text(encoding="utf-8"))
    parts = []
    names = (
        "csv_fed", "csv_acc",
        "json_fed", "json_acc",
        "json_array_fed", "json_array_acc",
        "json_ptr_fed", "json_ptr_acc",
    )
    for name in names:
        row = next((d for d in data if d.get("name") == name), None)
        if not row:
            parts.append(f"{name}=absent")
            continue
        err = (row.get("error_message") or "").replace("\n", " ")
        extra = f" error={err}" if err else ""
        parts.append(f"{name}={row.get('status')}{extra}")
    return " | ".join(parts)

def ready(label, step):
    p = ev / label / f"ready-{step}.txt"
    return p.read_text(encoding="utf-8").strip() if p.exists() else "missing"

def log_hits(label, name):
    p = ev / label / name
    if not p.exists():
        return "(no log slice)"
    text = p.read_text(encoding="utf-8").strip()
    return text if text else "(no WARN/ERROR lines in slice)"

lines = [
    "# File-connector trunk vs branch",
    "",
    "Same helper, same spicepod, same SQL. Sums are `amount_cents,user_id`.",
    "",
    "## Baseline",
    "",
]
DATASETS = (
    "csv_fed", "csv_acc",
    "json_fed", "json_acc",
    "json_array_fed", "json_array_acc",
    "json_ptr_fed", "json_ptr_acc",
)
for ds in DATASETS:
    lines.append(f"- `{ds}` trunk `{sums('trunk', 'baseline', ds)}` / branch `{sums('branch', 'baseline', ds)}`")
lines += [
    "",
    f"- trunk datasets: {datasets('trunk', 'baseline')}",
    f"- branch datasets: {datasets('branch', 'baseline')}",
    "",
    "## Reorder",
    "",
]
for step, ds in (
    ("reorder-fed", "csv_fed"),
    ("reorder-acc", "csv_acc"),
    ("reorder-fed", "json_fed"),
    ("reorder-acc", "json_acc"),
    ("reorder-fed", "json_array_fed"),
    ("reorder-acc", "json_array_acc"),
    ("reorder-fed", "json_ptr_fed"),
    ("reorder-acc", "json_ptr_acc"),
):
    lines.append(f"- `{ds}` {step} trunk `{sums('trunk', step, ds)}` / branch `{sums('branch', step, ds)}`")
lines += [
    "",
    f"- trunk datasets: {datasets('trunk', 'reorder')}",
    f"- branch datasets: {datasets('branch', 'reorder')}",
    "",
    "## Rename",
    "",
]
for step, ds in (
    ("rename-fed", "csv_fed"),
    ("rename-acc", "csv_acc"),
    ("rename-fed", "json_fed"),
    ("rename-acc", "json_acc"),
    ("rename-fed", "json_array_fed"),
    ("rename-acc", "json_array_acc"),
    ("rename-fed", "json_ptr_fed"),
    ("rename-acc", "json_ptr_acc"),
):
    lines.append(f"- `{ds}` {step} trunk `{sums('trunk', step, ds)}` / branch `{sums('branch', step, ds)}`")
lines += [
    "",
    f"- trunk `/v1/ready`: `{ready('trunk', 'rename')}`",
    f"- branch `/v1/ready`: `{ready('branch', 'rename')}`",
    f"- trunk datasets: {datasets('trunk', 'rename')}",
    f"- branch datasets: {datasets('branch', 'rename')}",
    "",
    "### Rename log slice (trunk)",
    "",
    "```",
    log_hits("trunk", "log-rename.txt"),
    "```",
    "",
    "### Rename log slice (branch)",
    "",
    "```",
    log_hits("branch", "log-rename.txt"),
    "```",
    "",
    "## Restore",
    "",
]
for step, ds in (
    ("restore-fed", "csv_fed"),
    ("restore-acc", "csv_acc"),
    ("restore-fed", "json_fed"),
    ("restore-acc", "json_acc"),
    ("restore-fed", "json_array_fed"),
    ("restore-acc", "json_array_acc"),
    ("restore-fed", "json_ptr_fed"),
    ("restore-acc", "json_ptr_acc"),
):
    lines.append(f"- `{ds}` {step} trunk `{sums('trunk', step, ds)}` / branch `{sums('branch', step, ds)}`")
lines += [
    "",
    f"- trunk datasets: {datasets('trunk', 'restore')}",
    f"- branch datasets: {datasets('branch', 'restore')}",
    "",
    "## Empty JSON",
    "",
]
for step, ds in (
    ("empty-fed", "json_fed"),
    ("empty-acc", "json_acc"),
    ("empty-fed", "json_array_fed"),
    ("empty-acc", "json_array_acc"),
    ("empty-fed", "json_ptr_fed"),
    ("empty-acc", "json_ptr_acc"),
    ("empty-acc", "csv_acc"),
):
    lines.append(f"- `{ds}` {step} trunk `{sums('trunk', step, ds)}` / branch `{sums('branch', step, ds)}`")
lines += [
    "",
    f"- trunk `/v1/ready`: `{ready('trunk', 'empty')}`",
    f"- branch `/v1/ready`: `{ready('branch', 'empty')}`",
    f"- trunk datasets: {datasets('trunk', 'empty')}",
    f"- branch datasets: {datasets('branch', 'empty')}",
    "",
]
report.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(report.read_text(encoding="utf-8"))
PY
}

run_one_side() {
  local label="$1"
  local binary="$2"
  local ev="$3"
  local work
  work="$(mktemp -d "/tmp/spice-file-connector-${label}.XXXXXX")"
  (
    HTTP_ADDR="127.0.0.1:$(free_port)"
    FLIGHT_ADDR="127.0.0.1:$(free_port)"
    METRICS_ADDR="127.0.0.1:$(free_port)"
    WORK="$work"
    SPICED="$binary"
    EVIDENCE="$ev"
    LABEL="$label"
    verify_status=0
    cleanup_on_exit() {
      local trap_status=$?
      local status="$verify_status"
      if (( status == 0 && trap_status != 0 )); then
        status="$trap_status"
      fi
      local cleanup_status=0
      # Nested subshell so `die` in cleanup cannot overwrite a verify failure.
      ( cmd_cleanup ) || cleanup_status=$?
      if (( status != 0 )); then
        exit "$status"
      fi
      if (( cleanup_status != 0 )); then
        echo "error: cleanup failed after successful ${label} verification (status ${cleanup_status})" >&2
        exit "$cleanup_status"
      fi
    }
    trap cleanup_on_exit EXIT
    cmd_seed
    cmd_launch
    cmd_doctor
    cmd_drive
  )
}

cmd_compare() {
  [[ -n "$TRUNK_SPICED" && -n "$BRANCH_SPICED" && -n "$EVIDENCE" ]] || \
    die "--trunk-spiced --branch-spiced --evidence are required"
  TRUNK_SPICED="$(resolve_spiced "$TRUNK_SPICED")"
  BRANCH_SPICED="$(resolve_spiced "$BRANCH_SPICED")"
  mkdir -p "$EVIDENCE"
  echo "compare trunk=${TRUNK_SPICED} branch=${BRANCH_SPICED}"
  run_one_side trunk "$TRUNK_SPICED" "$EVIDENCE"
  run_one_side branch "$BRANCH_SPICED" "$EVIDENCE"
  write_compare_report "$EVIDENCE"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  cmd="${1:-}"
  shift || true
  parse_args "$@"
  case "$cmd" in
    seed) cmd_seed ;;
    launch) cmd_launch ;;
    doctor) cmd_doctor ;;
    drive) cmd_drive ;;
    cleanup) cmd_cleanup ;;
    compare) cmd_compare ;;
    ""|-h|--help) usage ;;
    *) usage; die "unknown command: $cmd" ;;
  esac
fi
