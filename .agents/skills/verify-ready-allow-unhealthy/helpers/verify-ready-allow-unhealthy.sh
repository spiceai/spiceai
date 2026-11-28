#!/usr/bin/env bash
# Drive a real spiced and poll /v1/ready vs /v1/ready?allow_unhealthy=true.
# Subcommands: seed | launch | doctor | poll | cleanup | compare
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
verify-ready-allow-unhealthy.sh <seed|launch|doctor|poll|cleanup|compare> [flags]

  seed     --work DIR
  launch   --spiced PATH --work DIR [--http ADDR] [--flight ADDR] [--metrics ADDR]
  doctor   --work DIR --http ADDR [--spiced PATH]
  poll     --work DIR --http ADDR --evidence DIR --label LABEL [--seconds N]
  cleanup  --work DIR
  compare  --trunk-spiced PATH --branch-spiced PATH --evidence DIR [--seconds N]
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
SECONDS_N="60"
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
      --seconds) SECONDS_N="$2"; shift 2 ;;
      --trunk-spiced) TRUNK_SPICED="$2"; shift 2 ;;
      --branch-spiced) BRANCH_SPICED="$2"; shift 2 ;;
      --help|-h) usage; exit 0 ;;
      *) die "unknown flag: $1" ;;
    esac
  done
}

pidfile() { echo "${WORK}/spiced.pid"; }
logfile() { echo "${WORK}/spiced.log"; }
identityfile() { echo "${WORK}/spiced.identity"; }
source_pidfile() { echo "${WORK}/source.pid"; }
source_identityfile() { echo "${WORK}/source.identity"; }
source_portfile() { echo "${WORK}/source.port"; }

write_source_identity() {
  local pid="$1"
  local exe starttime
  exe="$(process_exe "$pid")"
  starttime="$(process_stat_field "$pid" 19)"
  [[ -n "$exe" && -n "$starttime" ]] || die "could not record identity for source pid $pid"
  printf 'pid=%s\nexe=%s\nstarttime=%s\n' "$pid" "$exe" "$starttime" > "$(source_identityfile)"
}

source_identity_matches() {
  local pid exe starttime got_exe got_start
  pid="$(cat "$(source_pidfile)" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  exe="$(awk -F= '/^exe=/{print substr($0,5)}' "$(source_identityfile)" 2>/dev/null || true)"
  starttime="$(awk -F= '/^starttime=/{print $2}' "$(source_identityfile)" 2>/dev/null || true)"
  [[ -n "$exe" && -n "$starttime" ]] || return 1
  got_exe="$(process_exe "$pid")"
  got_start="$(process_stat_field "$pid" 19)"
  [[ "$got_exe" == "$exe" && "$got_start" == "$starttime" ]]
}

stop_pid_if_ours() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  kill "$pid" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.25
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

write_spicepod() {
  local source_url="$1"
  cat > "$WORK/spicepod.yaml" <<YAML
version: v1
kind: Spicepod
name: verify-ready-allow-unhealthy
datasets:
  - from: file:data/nation.csv
    name: nation
    params:
      file_format: csv
  - from: ${source_url}
    name: missing
    params:
      file_format: csv
      client_timeout: 5
      connect_timeout: 2
    acceleration:
      enabled: true
      engine: cayenne
      mode: memory
      refresh_mode: full
      refresh_retry_enabled: true
YAML
}

start_failing_source() {
  require_cmd python3
  python3 - "$WORK/data/nation.csv" "$(source_portfile)" "$(source_pidfile)" <<'PY' &
import pathlib, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

csv_path, port_path, pid_path = sys.argv[1], sys.argv[2], sys.argv[3]
body = pathlib.Path(csv_path).read_bytes()
lock = threading.Lock()
gets = {"n": 0}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("%s\n" % (fmt % args))

    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

    def do_HEAD(self):
        self._ok()

    def do_GET(self):
        with lock:
            gets["n"] += 1
            n = gets["n"]
        if n == 1:
            self._ok()
            self.wfile.write(body)
            return
        # Slow-fail so /v1/datasets can observe Refreshing for more than one poll.
        time.sleep(2.5)
        msg = b"source refused after first schema read\n"
        self.send_response(500)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        self.wfile.write(msg)

httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
pathlib.Path(port_path).write_text(str(httpd.server_address[1]), encoding="utf-8")
pathlib.Path(pid_path).write_text(str(__import__("os").getpid()), encoding="utf-8")
httpd.serve_forever()
PY
  local py_pid=$!
  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    if [[ -s "$(source_portfile)" && -s "$(source_pidfile)" ]]; then
      break
    fi
    if ! kill -0 "$py_pid" 2>/dev/null; then
      die "source HTTP server exited during start"
    fi
    sleep 0.05
  done
  [[ -s "$(source_portfile)" ]] || die "source HTTP server did not write its port"
  local src_pid
  src_pid="$(cat "$(source_pidfile)")"
  write_source_identity "$src_pid"
  echo "source http://127.0.0.1:$(cat "$(source_portfile)") pid=$src_pid"
}

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

process_exe() {
  local pid="$1"
  readlink -f "/proc/${pid}/exe" 2>/dev/null || true
}

write_identity() {
  local pid="$1"
  local exe starttime
  exe="$(process_exe "$pid")"
  starttime="$(process_stat_field "$pid" 19)"
  [[ -n "$exe" && -n "$starttime" ]] || die "could not record identity for pid $pid"
  printf 'pid=%s\nexe=%s\nstarttime=%s\n' "$pid" "$exe" "$starttime" > "$(identityfile)"
}

identity_matches() {
  local pid exe starttime got_exe got_start
  pid="$(cat "$(pidfile)" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  exe="$(awk -F= '/^exe=/{print substr($0,5)}' "$(identityfile)" 2>/dev/null || true)"
  starttime="$(awk -F= '/^starttime=/{print $2}' "$(identityfile)" 2>/dev/null || true)"
  [[ -n "$exe" && -n "$starttime" ]] || return 1
  got_exe="$(process_exe "$pid")"
  got_start="$(process_stat_field "$pid" 19)"
  [[ "$got_exe" == "$exe" && "$got_start" == "$starttime" ]]
}

http_code() {
  local url="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "$url" || echo "000"
}

http_body() {
  local url="$1"
  curl -sS --max-time 2 "$url" || true
}

dataset_status() {
  local json="$1"
  local name="$2"
  python3 - "$json" "$name" <<'PY'
import json, sys
raw, name = sys.argv[1], sys.argv[2]
try:
    data = json.loads(raw)
except json.JSONDecodeError:
    print("unreadable")
    sys.exit(0)
rows = data if isinstance(data, list) else data.get("datasets") or data.get("data") or []
if not isinstance(rows, list):
    print("unreadable")
    sys.exit(0)
for row in rows:
    if not isinstance(row, dict):
        continue
    n = row.get("name") or row.get("dataset")
    if n == name:
        print(row.get("status") or row.get("state") or "unknown")
        sys.exit(0)
print("absent")
PY
}

cmd_seed() {
  parse_args "$@"
  [[ -n "$WORK" ]] || die "seed needs --work"
  mkdir -p "$WORK/data"
  WORK="$(cd "$WORK" && pwd)"
  cat > "$WORK/data/nation.csv" <<'CSV'
n_nationkey,n_name,n_regionkey
0,ALGERIA,0
1,ARGENTINA,1
CSV
  write_spicepod "http://127.0.0.1:9/nation.csv"
  echo "seeded $WORK"
}

cmd_launch() {
  parse_args "$@"
  require_cmd curl
  [[ -n "$SPICED" && -n "$WORK" ]] || die "launch needs --spiced and --work"
  [[ -f "$WORK/spicepod.yaml" ]] || die "seed $WORK first"
  SPICED="$(cd "$(dirname "$SPICED")" && pwd)/$(basename "$SPICED")"
  [[ -x "$SPICED" ]] || die "not executable: $SPICED"
  WORK="$(cd "$WORK" && pwd)"
  HTTP_ADDR="${HTTP_ADDR:-127.0.0.1:18490}"
  FLIGHT_ADDR="${FLIGHT_ADDR:-127.0.0.1:15451}"
  METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:19490}"

  if [[ -f "$(pidfile)" ]] && identity_matches; then
    die "already running in $WORK (pid $(cat "$(pidfile)"))"
  fi
  if curl -s -o /dev/null --max-time 2 "http://${HTTP_ADDR}/health"; then
    die "something is already listening on $HTTP_ADDR"
  fi

  start_failing_source
  write_spicepod "http://127.0.0.1:$(cat "$(source_portfile)")/nation.csv"

  (cd "$WORK" && exec "$SPICED" --telemetry-enabled=false \
    --http "$HTTP_ADDR" --flight "$FLIGHT_ADDR" --metrics "$METRICS_ADDR" . \
    > "$(logfile)" 2>&1) &
  local pid=$!
  echo "$pid" > "$(pidfile)"
  write_identity "$pid"
  echo "$SPICED" > "$WORK/spiced.binary"

  local deadline=$((SECONDS + 90))
  local health=""
  while (( SECONDS < deadline )); do
    if ! kill -0 "$pid" 2>/dev/null; then
      tail -n 40 "$(logfile)" >&2
      die "spiced exited during launch"
    fi
    health="$(http_code "http://${HTTP_ADDR}/health")"
    if [[ "$health" == "200" ]]; then
      echo "launched pid=$pid health=200 http=$HTTP_ADDR"
      return 0
    fi
    sleep 0.5
  done
  tail -n 40 "$(logfile)" >&2
  die "GET /health never answered 200 (last=$health)"
}

cmd_doctor() {
  parse_args "$@"
  require_cmd curl
  [[ -n "$WORK" && -n "$HTTP_ADDR" ]] || die "doctor needs --work and --http"
  WORK="$(cd "$WORK" && pwd)"
  identity_matches || die "identity does not match pid in $WORK"
  if [[ -n "$SPICED" ]]; then
    SPICED="$(cd "$(dirname "$SPICED")" && pwd)/$(basename "$SPICED")"
    local recorded
    recorded="$(awk -F= '/^exe=/{print substr($0,5)}' "$(identityfile)")"
    local want
    want="$(readlink -f "$SPICED")"
    [[ "$recorded" == "$want" ]] || die "identity exe $recorded != $want"
  fi
  local health ready
  health="$(http_code "http://${HTTP_ADDR}/health")"
  [[ "$health" == "200" ]] || die "GET /health -> $health"
  ready="$(http_code "http://${HTTP_ADDR}/v1/ready")"
  local datasets
  datasets="$(http_body "http://${HTTP_ADDR}/v1/datasets?status=true")"
  local nation missing
  nation="$(dataset_status "$datasets" nation)"
  missing="$(dataset_status "$datasets" missing)"
  [[ "$nation" != "absent" && "$nation" != "unreadable" ]] || die "nation not listed ($nation)"
  [[ "$missing" != "absent" && "$missing" != "unreadable" ]] || die "missing not listed ($missing)"
  echo "doctor ok health=$health ready=$ready nation=$nation missing=$missing"
}

cmd_poll() {
  parse_args "$@"
  require_cmd curl
  [[ -n "$WORK" && -n "$HTTP_ADDR" && -n "$EVIDENCE" && -n "$LABEL" ]] || \
    die "poll needs --work --http --evidence --label"
  WORK="$(cd "$WORK" && pwd)"
  mkdir -p "$EVIDENCE/$LABEL"
  local dest="$EVIDENCE/$LABEL"
  dest="$(cd "$dest" && pwd)"
  cp "$WORK/spicepod.yaml" "$dest/spicepod.yaml"
  mkdir -p "$dest/data"
  cp "$WORK/data/"* "$dest/data/"
  if [[ -f "$(logfile)" ]]; then
    cp "$(logfile)" "$dest/spiced.log.start" || true
  fi

  local sha="" repo=""
  repo="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
  if [[ -n "$repo" ]]; then
    sha="$(git -C "$repo" rev-parse HEAD 2>/dev/null || true)"
  fi
  local version=""
  if [[ -f "$WORK/spiced.binary" ]]; then
    version="$("$(cat "$WORK/spiced.binary")" --version 2>/dev/null | head -1 || true)"
  fi

  echo "unix,plain,allow_unhealthy,nation,missing" > "$dest/poll.csv"
  local prev_allow=""
  local flips=0
  local samples=0
  local allow_200=0
  local allow_503=0
  local plain_200=0
  local plain_503=0
  local deadline=$((SECONDS + SECONDS_N))
  while (( SECONDS < deadline )); do
    local ts plain allow datasets nation missing
    ts="$(date +%s)"
    plain="$(http_code "http://${HTTP_ADDR}/v1/ready")"
    allow="$(http_code "http://${HTTP_ADDR}/v1/ready?allow_unhealthy=true")"
    datasets="$(http_body "http://${HTTP_ADDR}/v1/datasets?status=true")"
    nation="$(dataset_status "$datasets" nation)"
    missing="$(dataset_status "$datasets" missing)"
    echo "$ts,$plain,$allow,$nation,$missing" >> "$dest/poll.csv"
    samples=$((samples + 1))
    case "$plain" in
      200) plain_200=$((plain_200 + 1)) ;;
      503) plain_503=$((plain_503 + 1)) ;;
    esac
    case "$allow" in
      200) allow_200=$((allow_200 + 1)) ;;
      503) allow_503=$((allow_503 + 1)) ;;
    esac
    if [[ -n "$prev_allow" && "$prev_allow" != "$allow" ]]; then
      flips=$((flips + 1))
    fi
    prev_allow="$allow"
    sleep 1
  done

  if [[ -f "$(logfile)" ]]; then
    cp "$(logfile)" "$dest/spiced.log"
  fi

  {
    echo "label=$LABEL"
    echo "git_head=${sha}"
    echo "binary_version=${version}"
    echo "http=$HTTP_ADDR"
    echo "seconds=$SECONDS_N"
    echo "samples=$samples"
    echo "plain_200=$plain_200"
    echo "plain_503=$plain_503"
    echo "allow_unhealthy_200=$allow_200"
    echo "allow_unhealthy_503=$allow_503"
    echo "allow_unhealthy_flips=$flips"
    echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$dest/summary.txt"

  python3 - "$dest/poll.csv" >> "$dest/summary.txt" <<'PY'
import collections, pathlib, sys
path = pathlib.Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()[1:]
missing = collections.Counter()
pairs = collections.Counter()
for line in lines:
    parts = line.split(",")
    if len(parts) < 5:
        continue
    missing[parts[4]] += 1
    pairs[(parts[1], parts[2], parts[3], parts[4])] += 1
print("missing_status_counts=" + ",".join(f"{k}:{v}" for k, v in sorted(missing.items())))
print("timeline_counts:")
for (plain, allow, nation, miss), n in sorted(pairs.items(), key=lambda kv: -kv[1]):
    print(f"  plain={plain} allow={allow} nation={nation} missing={miss} n={n}")
PY

  cat "$dest/summary.txt"
}

cmd_cleanup() {
  parse_args "$@"
  [[ -n "$WORK" ]] || die "cleanup needs --work"
  WORK="$(cd "$WORK" && pwd)"
  if [[ -f "$(pidfile)" ]]; then
    local pid
    pid="$(cat "$(pidfile)")"
    if identity_matches; then
      kill "$pid" 2>/dev/null || true
      local i
      for i in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.25
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
      fi
      wait "$pid" 2>/dev/null || true
    else
      echo "cleanup: identity mismatch; not signalling pid $pid" >&2
    fi
  fi
  rm -f "$(pidfile)" "$(identityfile)" "$WORK/spiced.binary"
  if [[ -f "$(source_pidfile)" ]]; then
    local src_pid
    src_pid="$(cat "$(source_pidfile)")"
    if source_identity_matches; then
      stop_pid_if_ours "$src_pid"
    else
      echo "cleanup: source identity mismatch; not signalling pid $src_pid" >&2
    fi
  fi
  rm -f "$(source_pidfile)" "$(source_identityfile)" "$(source_portfile)"
  rm -rf "$WORK/.spice"
}

run_one_side() {
  local spiced="$1"
  local label="$2"
  local evidence="$3"
  local seconds="$4"
  local http="$5"
  local flight="$6"
  local metrics="$7"
  local work
  work="$(mktemp -d "/tmp/spice-ready-allow-unhealthy-${label}-XXXX")"
  cleanup_side() { cmd_cleanup --work "$work" || true; }
  trap cleanup_side EXIT
  cmd_seed --work "$work"
  cmd_launch --spiced "$spiced" --work "$work" --http "$http" --flight "$flight" --metrics "$metrics"
  cmd_doctor --work "$work" --http "$http" --spiced "$spiced"
  cmd_poll --work "$work" --http "$http" --evidence "$evidence" --label "$label" --seconds "$seconds"
  cleanup_side
  trap - EXIT
}

cmd_compare() {
  parse_args "$@"
  [[ -n "$TRUNK_SPICED" && -n "$BRANCH_SPICED" && -n "$EVIDENCE" ]] || \
    die "compare needs --trunk-spiced --branch-spiced --evidence"
  TRUNK_SPICED="$(cd "$(dirname "$TRUNK_SPICED")" && pwd)/$(basename "$TRUNK_SPICED")"
  BRANCH_SPICED="$(cd "$(dirname "$BRANCH_SPICED")" && pwd)/$(basename "$BRANCH_SPICED")"
  mkdir -p "$EVIDENCE"
  EVIDENCE="$(cd "$EVIDENCE" && pwd)"
  run_one_side "$TRUNK_SPICED" trunk "$EVIDENCE" "$SECONDS_N" \
    127.0.0.1:18490 127.0.0.1:15451 127.0.0.1:19490
  run_one_side "$BRANCH_SPICED" branch "$EVIDENCE" "$SECONDS_N" \
    127.0.0.1:18491 127.0.0.1:15452 127.0.0.1:19491
  echo "wrote $EVIDENCE/trunk/summary.txt and $EVIDENCE/branch/summary.txt"
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    seed) cmd_seed "$@" ;;
    launch) cmd_launch "$@" ;;
    doctor) cmd_doctor "$@" ;;
    poll) cmd_poll "$@" ;;
    cleanup) cmd_cleanup "$@" ;;
    compare) cmd_compare "$@" ;;
    ""|-h|--help) usage ;;
    *) die "unknown command: $cmd" ;;
  esac
}

main "$@"
