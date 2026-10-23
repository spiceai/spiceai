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

seed_files() {
  local data="${WORK}/data"
  mkdir -p "$data"
  write_events_csv "${data}/events.csv" \
    "event_id,user_id,region,amount_cents,ts" \
    "1,10,us,100,t0" \
    "2,20,eu,200,t1"
  write_events_jsonl "${data}/events.jsonl" \
    '{"event_id":1,"user_id":10,"region":"us","amount_cents":100,"ts":"t0"}' \
    '{"event_id":2,"user_id":20,"region":"eu","amount_cents":200,"ts":"t1"}'
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
YAML
  seed_files
}

cmd_seed() {
  [[ -n "$WORK" ]] || die "--work is required"
  mkdir -p "$WORK"
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
  SPICED="$(resolve_spiced "$SPICED")"
  [[ -f "${WORK}/spicepod.yaml" ]] || seed_spicepod
  HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:$(free_port)}"
  FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:$(free_port)}"
  METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:$(free_port)}"
  mkdir -p "$WORK"
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
  printf '%s\n' "$HTTP_ADDR" >"${WORK}/http.addr"
  printf '%s\n' "$FLIGHT_ADDR" >"${WORK}/flight.addr"
  echo "started pid=${pid} http=${HTTP_ADDR} flight=${FLIGHT_ADDR} log=$(logfile)"
  wait_ready
}

cmd_doctor() {
  [[ -n "$WORK" && -n "$HTTP_ADDR" ]] || die "--work and --http are required"
  local pf
  pf="$(pidfile)"
  [[ -f "$pf" ]] || die "no pidfile at $pf"
  local pid
  pid="$(cat "$pf")"
  kill -0 "$pid" 2>/dev/null || die "pid $pid is not running"
  local exe
  exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
  if [[ -n "$SPICED" ]]; then
    local want
    want="$(readlink -f "$SPICED")"
    [[ "$exe" == "$want" ]] || die "pid $pid exe is ${exe}, expected ${want}"
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
needed = {"csv_fed", "csv_acc", "json_fed", "json_acc"}
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

wait_refresh_settled() {
  local name="$1"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    local json
    json="$(curl -sS --max-time 10 "http://${HTTP_ADDR}/v1/datasets?status=true")"
    local status
    status="$(python3 - "$json" "$name" <<'PY'
import json, sys
data = json.loads(sys.argv[1])
name = sys.argv[2]
for d in data:
    if d.get("name") == name:
        print(d.get("status") or "")
        raise SystemExit(0)
print("missing")
PY
)"
    if [[ "$status" != "Refreshing" && "$status" != "Initializing" && "$status" != "missing" ]]; then
      echo "refresh settled ${name} status=${status}"
      return 0
    fi
    sleep 0.25
  done
  die "dataset ${name} still Refreshing after 60s"
}

summarize_datasets() {
  python3 - "$1" <<'PY'
import json, sys
data = json.loads(open(sys.argv[1], encoding="utf-8").read())
parts = []
for name in ("csv_fed", "csv_acc", "json_fed", "json_acc"):
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
  [[ -n "$WORK" && -n "$HTTP_ADDR" && -n "$EVIDENCE" && -n "$LABEL" ]] || \
    die "--work --http --evidence --label are required"
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

  ready_capture "${ev}/ready-baseline.txt"
  datasets_status "${ev}/datasets-baseline.json"
  echo "baseline datasets: $(summarize_datasets "${ev}/datasets-baseline.json")" | tee -a "${ev}/summary.txt"
  for ds in csv_fed csv_acc json_fed json_acc; do
    query_dataset baseline "$ds" "$ev"
  done

  write_events_csv "${WORK}/data/events.csv" \
    "event_id,amount_cents,region,user_id,ts" \
    "1,100,us,10,t0" \
    "2,200,eu,20,t1"
  write_events_jsonl "${WORK}/data/events.jsonl" \
    '{"event_id":1,"amount_cents":100,"region":"us","user_id":10,"ts":"t0"}' \
    '{"event_id":2,"amount_cents":200,"region":"eu","user_id":20,"ts":"t1"}'
  cp "${WORK}/data/events.csv" "${ev}/events.reordered.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.reordered.jsonl"

  query_dataset reorder-fed csv_fed "$ev"
  query_dataset reorder-fed json_fed "$ev"
  refresh_dataset csv_acc "${ev}/refresh-reorder-csv_acc"
  refresh_dataset json_acc "${ev}/refresh-reorder-json_acc"
  wait_refresh_settled csv_acc
  wait_refresh_settled json_acc
  query_dataset reorder-acc csv_acc "$ev"
  query_dataset reorder-acc json_acc "$ev"
  datasets_status "${ev}/datasets-reorder.json"
  ready_capture "${ev}/ready-reorder.txt"
  echo "reorder datasets: $(summarize_datasets "${ev}/datasets-reorder.json")" | tee -a "${ev}/summary.txt"

  local log_at
  log_at="$(wc -c <"$(logfile)" | tr -d ' ')"
  write_events_csv "${WORK}/data/events.csv" \
    "event_id,user_id,region,amt,ts" \
    "1,10,us,100,t0" \
    "2,20,eu,200,t1"
  write_events_jsonl "${WORK}/data/events.jsonl" \
    '{"event_id":1,"user_id":10,"region":"us","amt":100,"ts":"t0"}' \
    '{"event_id":2,"user_id":20,"region":"eu","amt":200,"ts":"t1"}'
  cp "${WORK}/data/events.csv" "${ev}/events.renamed.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.renamed.jsonl"

  query_dataset rename-fed csv_fed "$ev"
  query_dataset rename-fed json_fed "$ev"
  capture_log_slice "$(logfile)" "$log_at" "${ev}/log-rename-fed.txt"
  refresh_dataset csv_acc "${ev}/refresh-rename-csv_acc"
  refresh_dataset json_acc "${ev}/refresh-rename-json_acc"
  wait_refresh_settled csv_acc
  wait_refresh_settled json_acc
  query_dataset rename-acc csv_acc "$ev"
  query_dataset rename-acc json_acc "$ev"
  datasets_status "${ev}/datasets-rename.json"
  ready_capture "${ev}/ready-rename.txt"
  capture_log_slice "$(logfile)" "$log_at" "${ev}/log-rename.txt"
  echo "rename datasets: $(summarize_datasets "${ev}/datasets-rename.json")" | tee -a "${ev}/summary.txt"

  write_events_csv "${WORK}/data/events.csv" \
    "event_id,user_id,region,amount_cents,ts" \
    "1,10,us,100,t0" \
    "2,20,eu,200,t1"
  write_events_jsonl "${WORK}/data/events.jsonl" \
    '{"event_id":1,"user_id":10,"region":"us","amount_cents":100,"ts":"t0"}' \
    '{"event_id":2,"user_id":20,"region":"eu","amount_cents":200,"ts":"t1"}'
  cp "${WORK}/data/events.csv" "${ev}/events.restored.csv"
  cp "${WORK}/data/events.jsonl" "${ev}/events.restored.jsonl"

  query_dataset restore-fed csv_fed "$ev"
  query_dataset restore-fed json_fed "$ev"
  refresh_dataset csv_acc "${ev}/refresh-restore-csv_acc"
  refresh_dataset json_acc "${ev}/refresh-restore-json_acc"
  wait_refresh_settled csv_acc
  wait_refresh_settled json_acc
  query_dataset restore-acc csv_acc "$ev"
  query_dataset restore-acc json_acc "$ev"
  datasets_status "${ev}/datasets-restore.json"
  ready_capture "${ev}/ready-restore.txt"
  echo "restore datasets: $(summarize_datasets "${ev}/datasets-restore.json")" | tee -a "${ev}/summary.txt"
  cp "$(logfile)" "${ev}/spiced.log"
  echo "drive complete ${ev}"
}

cmd_cleanup() {
  [[ -n "$WORK" ]] || die "--work is required"
  local pf
  pf="$(pidfile)"
  if [[ -f "$pf" ]]; then
    local pid
    pid="$(cat "$pf")"
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      local i
      for i in $(seq 1 40); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$pf"
    echo "stopped pid=${pid}"
  else
    echo "no pidfile; nothing to stop"
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
    for name in ("csv_fed", "csv_acc", "json_fed", "json_acc"):
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
for ds in ("csv_fed", "csv_acc", "json_fed", "json_acc"):
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
):
    lines.append(f"- `{ds}` {step} trunk `{sums('trunk', step, ds)}` / branch `{sums('branch', step, ds)}`")
lines += [
    "",
    f"- trunk datasets: {datasets('trunk', 'restore')}",
    f"- branch datasets: {datasets('branch', 'restore')}",
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
  HTTP_ADDR="127.0.0.1:$(free_port)"
  FLIGHT_ADDR="127.0.0.1:$(free_port)"
  METRICS_ADDR="127.0.0.1:$(free_port)"
  WORK="$work"
  SPICED="$binary"
  cmd_seed
  cmd_launch
  cmd_doctor
  EVIDENCE="$ev"
  LABEL="$label"
  cmd_drive
  cmd_cleanup
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
