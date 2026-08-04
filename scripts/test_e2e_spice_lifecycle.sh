#!/usr/bin/env bash
#
# Unit tests for the two scripts behind the E2E runtime lifecycle actions:
# `.github/actions/stop-spice/stop_spice.sh` and
# `.github/actions/wait-for-spice-ready/wait_for_ready.sh`.
#
# The bug they exist for (#12058) is not visible in the job that fails. A
# cleanup that reaps `spice` but not the `spiced` child leaves a listener on
# :8090, and the reused self-hosted runner hands that orphan to the *next* job,
# whose runtime cannot bind and never reports ready.
#
# The correction has to stay narrow, though: several runner instances share one
# physical machine (`<machine>-NN`, working out of `/opt/github-runner-NN/_work`)
# and therefore one network namespace, so a name-wide kill would reap a
# concurrent job's runtime on the same host. Hence three properties under test:
# stop really stops both processes, stop touches nothing outside this runner's
# workspace, and a wait that times out says why.
#
# No processes are started and no ports are bound: stub `pgrep`/`kill`/`lsof`/
# `curl` on PATH model whatever state a case needs.
#
# Usage: scripts/test_e2e_spice_lifecycle.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
stop_script="$repo_root/.github/actions/stop-spice/stop_spice.sh"
wait_script="$repo_root/.github/actions/wait-for-spice-ready/wait_for_ready.sh"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

for required in "$stop_script" "$wait_script"; do
  [ -f "$required" ] || { echo "missing subject: $required" >&2; exit 1; }
done

stub_dir="$(mktemp -d)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir" "$work_dir"' EXIT

# The two workspaces in play: ours, and a second runner instance on the same
# machine whose processes must survive everything this script does.
OURS="/opt/github-runner-02/_work/spiceai"
THEIRS="/opt/github-runner-03/_work/spiceai"

# ---------------------------------------------------------------------------
# Stubs.
#
# Process state is $STATE_DIR/procs, one "pid name cwd" per line. `kill` removes
# matching pids, so a test can assert exactly which PIDs were signalled and that
# the other instance's were not. STUB_IGNORE_TERM models a runtime that declines
# to die on SIGTERM, forcing the SIGKILL escalation. Signals are appended to
# $STATE_DIR/signals.
# ---------------------------------------------------------------------------

cat >"$stub_dir/pgrep" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
name="${!#}"
found=0
while read -r pid pname _cwd; do
  [ "$pname" = "$name" ] || continue
  echo "$pid"; found=1
done < "${STATE_DIR}/procs"
[ "$found" = "1" ]
STUB

cat >"$stub_dir/kill" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
sig="${1#-}"; shift
for pid in "$@"; do
  echo "kill ${sig} ${pid}" >> "${STATE_DIR}/signals"
  if [ "$sig" = "TERM" ] && [ -n "${STUB_IGNORE_TERM:-}" ]; then
    continue   # signal delivered, process declines to die
  fi
  grep -v "^${pid} " "${STATE_DIR}/procs" > "${STATE_DIR}/procs.tmp" 2>/dev/null || true
  mv "${STATE_DIR}/procs.tmp" "${STATE_DIR}/procs"
done
STUB

# Serves two roles the subjects use: `-d cwd -Fn` reports a process's working
# directory, and `-iTCP` reports port listeners (STUB_PORT_HOLDER, or nothing).
cat >"$stub_dir/lsof" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
if [[ "$*" == *"-d cwd"* ]]; then
  pid=""
  while [ $# -gt 0 ]; do
    [ "$1" = "-p" ] && { pid="$2"; break; }
    shift
  done
  while read -r p _name cwd; do
    if [ "$p" = "$pid" ]; then
      [ "$cwd" = "UNKNOWN" ] && exit 1
      echo "p${p}"; echo "n${cwd}"; exit 0
    fi
  done < "${STATE_DIR}/procs"
  exit 1
fi
[[ -n "${STUB_PORT_HOLDER:-}" ]] || exit 1
if [[ "$*" == *-t* ]]; then
  echo "${STUB_PORT_HOLDER}"
else
  echo "COMMAND   PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME"
  echo "spiced  ${STUB_PORT_HOLDER} runner  10u  IPv4 0x1234      0t0  TCP 127.0.0.1:8090 (LISTEN)"
fi
exit 0
STUB

# Answers with STUB_READY_BODY. STUB_READY_AFTER makes it fail N times first,
# modelling a runtime that comes up partway through the wait.
#
# STUB_READY_STATUS models the real endpoint: a runtime that is up but not ready
# answers 503 *with a body* saying so. This stub reproduces curl's `-f`
# behaviour of suppressing that body, which is why passing `-f` would blind the
# diagnostics — the reason the subject must not use it.
cat >"$stub_dir/curl" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
n=0
[[ -f "${STATE_DIR}/curl_calls" ]] && n="$(cat "${STATE_DIR}/curl_calls")"
n=$((n + 1))
echo "$n" > "${STATE_DIR}/curl_calls"
if [[ -n "${STUB_READY_AFTER:-}" && "$n" -lt "${STUB_READY_AFTER}" ]]; then
  exit 7   # connection refused
fi
status="${STUB_READY_STATUS:-200}"
if [[ "$status" -ge 400 ]]; then
  # `-f`: no body, exit 22. Without it: body on stdout, exit 0.
  [[ "$*" == *-f* ]] && exit 22
  printf '%s' "${STUB_READY_BODY:-}"
  exit 0
fi
printf '%s' "${STUB_READY_BODY:-}"
exit 0
STUB

chmod +x "$stub_dir"/*

# reset_state "<pid> <name> <cwd>" ...
reset_state() {
  : > "$stub_dir/procs"
  : > "$stub_dir/signals"
  rm -f "$stub_dir/curl_calls"
  local row
  for row in "$@"; do echo "$row" >> "$stub_dir/procs"; done
}

# Surviving PIDs in file order (which is the order the case declared them).
alive_pids() {
  local pid rest out=""
  while read -r pid rest; do
    [ -n "$pid" ] && out="${out:+${out} }${pid}"
  done < "$stub_dir/procs"
  printf '%s' "$out"
}

# Runs a subject with the stubs first on PATH. Echoes "<rc>|<output>".
#
# `enable -n kill` then `source`: `kill` is a shell builtin, so a stub on PATH
# would never be reached and the subject's signals would go to the real process
# table. Disabling the builtin routes them to the stub, which is the only way to
# assert *which* PIDs were signalled — the property this whole file exists for.
run_subject() {
  local subject="$1"
  shift
  local output rc
  output="$(cd "$work_dir" && env "PATH=$stub_dir:$PATH" "STATE_DIR=$stub_dir" "$@" \
    bash -c 'enable -n kill; source "$1"' _ "$subject" 2>&1)"
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

assert_contains() {
  local name="$1" haystack="$2" needle="$3"
  tests_run=$((tests_run + 1))
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "  ok: $name"
  else
    fail_test "$name — expected output to contain '${needle}'; got: ${haystack}"
  fi
}

assert_not_contains() {
  local name="$1" haystack="$2" needle="$3"
  tests_run=$((tests_run + 1))
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "  ok: $name"
  else
    fail_test "$name — expected output NOT to contain '${needle}'; got: ${haystack}"
  fi
}

assert_eq() {
  local name="$1" got="$2" want="$3"
  tests_run=$((tests_run + 1))
  if [[ "$got" == "$want" ]]; then
    echo "  ok: $name"
  else
    fail_test "$name — expected '${want}', got '${got}'"
  fi
}

stop_env=(SPICE_STOP_GRACE=2 SPICE_STOP_PORT_WAIT=2 "RUNNER_WORKSPACE=$OURS")

# ---------------------------------------------------------------------------
echo "stop_spice — reaping our own runtime"
# ---------------------------------------------------------------------------

# The regression itself: `spiced` holds the port, so a stop that only signals
# `spice` is the bug. Assert both PIDs were signalled.
reset_state "100 spice $OURS" "101 spiced $OURS"
result="$(run_subject "$stop_script" "${stop_env[@]}")"
signals="$(cat "$stub_dir/signals")"
assert_contains "signals the spiced child, not just the CLI" "$signals" "kill TERM 101"
assert_contains "signals the spice CLI too" "$signals" "kill TERM 100"
assert_eq "leaves no process of ours alive" "$(alive_pids)" ""
assert_eq "exits 0 on a clean stop" "${result%%|*}" "0"

# A leaked `spiced` with no CLI parent is exactly what a previous job hands over.
reset_state "101 spiced $OURS"
run_subject "$stop_script" "${stop_env[@]}" >/dev/null
assert_contains "reaps an orphaned spiced with no CLI parent" "$(cat "$stub_dir/signals")" "kill TERM 101"
assert_eq "the orphan is gone" "$(alive_pids)" ""

# Cleanup runs under `if: always()`; a job with nothing running must not fail.
reset_state
result="$(run_subject "$stop_script" "${stop_env[@]}")"
assert_eq "exits 0 when nothing is running" "${result%%|*}" "0"
assert_contains "says nothing of ours was running" "${result#*|}" "no spice/spiced process of ours"

# A runtime that ignores SIGTERM must be escalated, or the port stays held.
reset_state "100 spice $OURS" "101 spiced $OURS"
result="$(run_subject "$stop_script" "${stop_env[@]}" STUB_IGNORE_TERM=1)"
signals="$(cat "$stub_dir/signals")"
assert_contains "escalates to SIGKILL when TERM is ignored" "$signals" "kill KILL 101"
assert_contains "reports the escalation" "${result#*|}" "sending SIGKILL"
assert_eq "still exits 0 after escalating" "${result%%|*}" "0"

# ---------------------------------------------------------------------------
echo "stop_spice — leaving other runner instances alone"
# ---------------------------------------------------------------------------

# The hazard that makes a name-wide killall unacceptable: another instance on
# the same machine is mid-run. Its PIDs must not be touched.
reset_state "100 spice $OURS" "101 spiced $OURS" "200 spice $THEIRS" "201 spiced $THEIRS"
result="$(run_subject "$stop_script" "${stop_env[@]}")"
signals="$(cat "$stub_dir/signals")"
assert_contains "still stops our own spiced" "$signals" "kill TERM 101"
assert_not_contains "does not signal the other instance's spiced" "$signals" "201"
assert_not_contains "does not signal the other instance's CLI" "$signals" "200"
assert_eq "the other instance survives intact" "$(alive_pids)" "200 201"
assert_eq "exits 0 alongside a concurrent job" "${result%%|*}" "0"

# Only the other instance is running: we must reap nothing at all.
reset_state "200 spice $THEIRS" "201 spiced $THEIRS"
result="$(run_subject "$stop_script" "${stop_env[@]}")"
assert_eq "signals nothing when only another instance runs" "$(cat "$stub_dir/signals")" ""
assert_eq "that instance is untouched" "$(alive_pids)" "200 201"
# "running, but not ours" must read differently from "nothing running at all":
# a scope that never matches would silently restore the leak, so it has to be
# visible in the log rather than look like an idle runner.
assert_contains "distinguishes out-of-scope from nothing-running" "${result#*|}" "none of it is under $OURS"
assert_not_contains "does not claim nothing was running" "${result#*|}" "no spice/spiced process of ours"

# An undeterminable cwd must not be killed — leaking costs a retry, killing a
# concurrent job destroys in-flight work.
reset_state "300 spiced UNKNOWN"
result="$(run_subject "$stop_script" "${stop_env[@]}")"
assert_eq "does not kill a process whose cwd is unknown" "$(cat "$stub_dir/signals")" ""
assert_contains "says why it refused" "${result#*|}" "cannot determine cwd of PID 300"
assert_eq "the unknown process survives" "$(alive_pids)" "300"

# Run by hand with no RUNNER_WORKSPACE, "stop spice" means all of it.
reset_state "100 spice $OURS" "201 spiced $THEIRS"
result="$(run_subject "$stop_script" SPICE_STOP_GRACE=2 SPICE_STOP_PORT_WAIT=2)"
assert_contains "unscoped run stops everything" "$(cat "$stub_dir/signals")" "kill TERM 201"
assert_contains "announces it is unscoped" "${result#*|}" "no RUNNER_WORKSPACE"

# ---------------------------------------------------------------------------
echo "stop_spice — port reporting"
# ---------------------------------------------------------------------------

reset_state
result="$(run_subject "$stop_script" "${stop_env[@]}")"
assert_contains "confirms the http port is free" "${result#*|}" ":8090 is free"
assert_contains "confirms the flight port is free" "${result#*|}" ":50051 is free"

# A port still held once our processes are gone is the shared-machine
# collision, and must be described as that rather than as a leak.
reset_state
result="$(run_subject "$stop_script" SPICE_STOP_GRACE=1 SPICE_STOP_PORT_WAIT=1 \
  "RUNNER_WORKSPACE=$OURS" STUB_PORT_HOLDER=4321)"
assert_contains "reports a port held by someone else" "${result#*|}" "none of them ours"
assert_contains "names the holding pid" "${result#*|}" "4321"
assert_contains "attributes it to another runner instance" "${result#*|}" "another runner instance's job, not a leak"
assert_eq "a contended port still exits 0" "${result%%|*}" "0"

reset_state
result="$(run_subject "$stop_script" SPICE_STOP_GRACE=1 SPICE_STOP_PORT_WAIT=1 \
  "RUNNER_WORKSPACE=$OURS" SPICE_STOP_PORTS="9999")"
assert_contains "honours SPICE_STOP_PORTS" "${result#*|}" ":9999 is free"
assert_not_contains "does not check unlisted ports" "${result#*|}" ":8090"

# ---------------------------------------------------------------------------
echo "wait_for_ready"
# ---------------------------------------------------------------------------

reset_state "101 spiced $OURS"
result="$(run_subject "$wait_script" STUB_READY_BODY=ready SPICE_READY_TIMEOUT=5)"
assert_eq "exits 0 once the runtime reports ready" "${result%%|*}" "0"
assert_contains "reports how long it took" "${result#*|}" "Runtime ready after"

# Not-ready must keep waiting rather than pass — the pre-#12057 expect bug was
# exactly this shape, where an absent process read as success.
reset_state "101 spiced $OURS"
result="$(run_subject "$wait_script" STUB_READY_BODY=initializing SPICE_READY_TIMEOUT=3 SPICE_READY_INTERVAL=1)"
assert_eq "fails when the runtime never becomes ready" "${result%%|*}" "1"
assert_contains "quotes what the endpoint actually said" "${result#*|}" "initializing"

# A runtime that comes up mid-wait must be caught, not timed out.
reset_state "101 spiced $OURS"
result="$(run_subject "$wait_script" STUB_READY_BODY=ready STUB_READY_AFTER=3 SPICE_READY_TIMEOUT=20 SPICE_READY_INTERVAL=1)"
assert_eq "waits through connection refused, then succeeds" "${result%%|*}" "0"

# The real not-ready response is 503 *with* a body. Reporting that body is the
# entire diagnostic value here, and `curl -f` would throw it away — so this is
# the case that pins the absence of `-f`.
reset_state "101 spiced $OURS"
result="$(run_subject "$wait_script" STUB_READY_BODY="not ready" STUB_READY_STATUS=503 \
  SPICE_READY_TIMEOUT=3 SPICE_READY_INTERVAL=1)"
out="${result#*|}"
assert_eq "a 503 keeps the wait going" "${result%%|*}" "1"
assert_contains "reports the body of a 503, not an empty response" "$out" "not ready"
assert_not_contains "does not report a 503 as an empty response" "$out" "(empty"

# The whole point of #12058: a timeout must name its cause.
reset_state
printf 'ERROR spiced: Unable to start HTTP server: Unable to bind to address: Address already in use (os error 48)\n' \
  > "$work_dir/spice.log"
result="$(run_subject "$wait_script" SPICE_READY_TIMEOUT=2 SPICE_READY_INTERVAL=1 STUB_PORT_HOLDER=7777)"
out="${result#*|}"
assert_eq "times out non-zero" "${result%%|*}" "1"
assert_contains "dumps spice.log" "$out" "Address already in use"
assert_contains "notices the runtime is not running" "$out" "the runtime is not running"
assert_contains "names what holds the port" "$out" "7777"
assert_contains "explains the contended-port case" "$out" "already held it"
assert_contains "points at the tracking issue" "$out" "#12058"
rm -f "$work_dir/spice.log"

# A live-but-not-ready runtime is a different diagnosis from a dead one.
reset_state "101 spiced $OURS"
result="$(run_subject "$wait_script" STUB_READY_BODY= SPICE_READY_TIMEOUT=2 SPICE_READY_INTERVAL=1)"
out="${result#*|}"
assert_contains "lists the live spiced process" "$out" "101"
assert_not_contains "does not claim a live runtime exited" "$out" "the runtime is not running"
assert_contains "reports an empty response distinctly" "$out" "(empty"

# A missing log must not crash the diagnosis.
reset_state
result="$(run_subject "$wait_script" SPICE_READY_TIMEOUT=2 SPICE_READY_INTERVAL=1)"
assert_eq "still exits 1 with no log present" "${result%%|*}" "1"
assert_contains "says the log is missing" "${result#*|}" "no spice.log"

# A non-default endpoint must be diagnosed against its own port.
reset_state
result="$(run_subject "$wait_script" SPICE_READY_URL="http://localhost:9091/v1/ready" SPICE_READY_TIMEOUT=2 SPICE_READY_INTERVAL=1)"
assert_contains "diagnoses the configured port" "${result#*|}" "listeners on :9091"

# Ready answered by somebody else. Runner instances share a network namespace,
# so if our spiced lost the bind, this poll can be satisfied by another job's
# runtime and every later step would silently test *that* one. Our own log
# saying we failed to bind has to outrank whatever answered the socket.
reset_state "201 spiced $THEIRS"
printf 'ERROR spiced: Unable to bind to address: Address already in use (os error 48)\n' \
  > "$work_dir/spice.log"
result="$(run_subject "$wait_script" STUB_READY_BODY=ready SPICE_READY_TIMEOUT=5 SPICE_READY_INTERVAL=1)"
out="${result#*|}"
assert_eq "refuses a ready answered by another job's runtime" "${result%%|*}" "1"
assert_contains "says whose runtime it is not" "$out" "not this job's runtime"
assert_not_contains "does not claim our runtime came up" "$out" "Runtime ready after"
rm -f "$work_dir/spice.log"

# ...but a genuine ready alongside an unrelated log must still pass, or the
# guard above would fail every job whose log merely mentions the phrase.
reset_state "101 spiced $OURS"
printf 'INFO runtime: all good here\n' > "$work_dir/spice.log"
result="$(run_subject "$wait_script" STUB_READY_BODY=ready SPICE_READY_TIMEOUT=5)"
assert_eq "still accepts a genuine ready" "${result%%|*}" "0"
rm -f "$work_dir/spice.log"

echo
if [ "$failures" -eq 0 ]; then
  echo "all ${tests_run} tests passed"
  exit 0
fi
echo "${failures} of ${tests_run} tests failed"
exit 1
