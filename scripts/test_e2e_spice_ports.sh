#!/usr/bin/env bash
#
# Tests for .github/actions/allocate-spice-ports/allocate_ports.sh — the script
# that gives each E2E job its own spiced ports so jobs sharing a self-hosted
# host cannot collide on 8090/50051 (#12419).
#
# What matters here is not the specific number it picks but the properties the
# collision fix rests on: two runner instances must not be handed the same pair,
# the same instance must be handed the same pair every time, the pair must stay
# clear of the OS ephemeral range, and a port that is already held must be
# stepped over rather than handed out.
#
#   bash scripts/test_e2e_spice_ports.sh

set -uo pipefail

repo_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
script="$repo_root/.github/actions/allocate-spice-ports/allocate_ports.sh"

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

failures=0

pass() {
  printf '  ok    %s\n' "$1"
}

fail() {
  printf '  FAIL  %s\n' "$1"
  failures=$((failures + 1))
}

assert_eq() {
  # assert_eq <what> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass "$1"
  else
    fail "$1: expected '$2', got '$3'"
  fi
}

assert_ne() {
  # assert_ne <what> <a> <b>
  if [ "$2" != "$3" ]; then
    pass "$1"
  else
    fail "$1: both were '$2'"
  fi
}

assert_between() {
  # assert_between <what> <low> <high> <value>
  if [ "$4" -ge "$2" ] && [ "$4" -le "$3" ]; then
    pass "$1"
  else
    fail "$1: $4 is outside $2..$3"
  fi
}

# run_alloc <runner-name> [extra env assignments...] — runs the script with a
# fresh GITHUB_ENV and leaves the resulting assignments in $alloc_env, the
# stdout in $alloc_output.
alloc_env=""
alloc_output=""
run_alloc() {
  local runner="$1"
  shift

  local env_file="$work_dir/github_env"
  : >"$env_file"

  alloc_output=$(env RUNNER_NAME="$runner" GITHUB_ENV="$env_file" "$@" bash "$script" 2>&1)
  alloc_env=$(cat "$env_file")
}

# value_of <key> — reads a KEY=VALUE out of the last run's GITHUB_ENV.
value_of() {
  printf '%s\n' "$alloc_env" | sed -n "s/^$1=//p"
}

printf 'case: exports the four variables a job needs\n'
run_alloc 'github-runner-01'
for key in SPICE_HTTP_PORT SPICE_FLIGHT_PORT SPICE_HTTP_ENDPOINT SPICE_FLIGHT_ENDPOINT; do
  if [ -n "$(value_of "$key")" ]; then
    pass "exports $key"
  else
    fail "did not export $key"
  fi
done

printf 'case: the endpoints agree with the ports\n'
http_port=$(value_of SPICE_HTTP_PORT)
flight_port=$(value_of SPICE_FLIGHT_PORT)
assert_eq 'HTTP endpoint carries the http:// scheme and the HTTP port' \
  "http://127.0.0.1:${http_port}" "$(value_of SPICE_HTTP_ENDPOINT)"
# `spice run --flight-endpoint` is handed to `spiced --flight` verbatim, and
# that flag takes a bind address. A scheme here would reach spiced as part of
# the address and fail the bind.
assert_eq 'Flight endpoint is a bare host:port, with no scheme' \
  "127.0.0.1:${flight_port}" "$(value_of SPICE_FLIGHT_ENDPOINT)"

printf 'case: ports avoid spiced defaults and the OS ephemeral range\n'
# Linux allocates ephemeral ports from 32768 and macOS from 49152, so staying
# under 32768 keeps an unrelated outbound connection from taking the port first.
assert_between 'HTTP port sits in the reserved window' 20000 23999 "$http_port"
assert_between 'Flight port sits in the reserved window' 25000 28999 "$flight_port"
assert_ne 'HTTP port is not spiced default 8090' '8090' "$http_port"
assert_ne 'Flight port is not spiced default 50051' '50051' "$flight_port"

printf 'case: the same runner instance is handed the same pair every time\n'
run_alloc 'github-runner-01' GITHUB_RUN_ID=100 GITHUB_JOB=test_openai_model
first_http=$(value_of SPICE_HTTP_PORT)
run_alloc 'github-runner-01' GITHUB_RUN_ID=100 GITHUB_JOB=test_openai_model
assert_eq 'a repeated allocation is stable' "$first_http" "$(value_of SPICE_HTTP_PORT)"

printf 'case: runner instances sharing a host get different pairs\n'
# This is the whole point: -01 and -02 live on one machine and share its network
# namespace, so identical ports here would reproduce the bug being fixed.
declare -a seen=()
collisions=0
for instance in 01 02 03 04 05 06 07 08; do
  run_alloc "github-runner-${instance}" GITHUB_RUN_ID=100 GITHUB_JOB=test_openai_model
  port=$(value_of SPICE_HTTP_PORT)
  for previous in ${seen[@]+"${seen[@]}"}; do
    if [ "$previous" = "$port" ]; then
      collisions=$((collisions + 1))
    fi
  done
  seen+=("$port")
done
assert_eq 'eight runner instances on one host get eight distinct ports' '0' "$collisions"

printf 'case: a held port is stepped over\n'
# Hold the port this seed would otherwise pick, then ask again: the script must
# hand out a different one rather than a port spiced cannot bind.
run_alloc 'github-runner-holder' GITHUB_RUN_ID=200 GITHUB_JOB=test_hf_model
wanted_http=$(value_of SPICE_HTTP_PORT)

# A listener that stays up for the duration of the probe. `nc -l` differs
# between the BSD and GNU builds, so bind with whatever the runner has.
holder_pid=''
if command -v python3 >/dev/null 2>&1; then
  python3 - "$wanted_http" <<'PY' &
import socket, sys, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1])))
s.listen(1)
time.sleep(30)
PY
  holder_pid=$!
fi

if [ -n "$holder_pid" ]; then
  # Give the listener a moment to be visible to lsof/netstat.
  sleep 1

  if command -v lsof >/dev/null 2>&1 || command -v netstat >/dev/null 2>&1; then
    run_alloc 'github-runner-holder' GITHUB_RUN_ID=200 GITHUB_JOB=test_hf_model
    assert_ne 'a held port is not handed out' "$wanted_http" "$(value_of SPICE_HTTP_PORT)"
    case $alloc_output in
    *'already held'*) pass 'says why it moved on' ;;
    *) fail "output does not mention the held port: $alloc_output" ;;
    esac
  else
    printf '  skip  no lsof or netstat on this host, cannot test the probe\n'
  fi

  kill "$holder_pid" 2>/dev/null
  wait "$holder_pid" 2>/dev/null
else
  printf '  skip  no python3 to hold a port with\n'
fi

# ---------------------------------------------------------------------------
# stop-spice checks the ports this job actually used
# ---------------------------------------------------------------------------
#
# Cleanup waits for "the ports" to come free. Once a job binds its own pair,
# waiting on 8090/50051 instead would watch another runner instance's job — and
# never confirm ours was released. The full stop-spice behaviour is covered by
# scripts/test_e2e_spice_lifecycle.sh; this covers only which ports it picks.

stop_script="$repo_root/.github/actions/stop-spice/stop_spice.sh"

# Run in an empty directory with no spice processes to find, so the script falls
# straight through to its port report. `RUNNER_WORKSPACE` scopes it to a path
# that owns nothing, which keeps it from touching anything on this machine.
run_stop() {
  local empty="$work_dir/empty"
  mkdir -p "$empty"
  (cd "$empty" && env RUNNER_WORKSPACE="$empty" SPICE_STOP_PORT_WAIT=0 SPICE_STOP_GRACE=0 \
    "$@" bash "$stop_script" 2>&1)
}

printf 'case: stop-spice checks the ports the job allocated\n'
stop_output=$(run_stop SPICE_HTTP_PORT=21734 SPICE_FLIGHT_PORT=26734)
case $stop_output in
*':21734'*) pass 'checks the allocated HTTP port' ;;
*) fail "does not mention :21734: $stop_output" ;;
esac
case $stop_output in
*':26734'*) pass 'checks the allocated Flight port' ;;
*) fail "does not mention :26734: $stop_output" ;;
esac
case $stop_output in
*':8090'*) fail "still checks the default :8090: $stop_output" ;;
*) pass 'does not check the default :8090' ;;
esac

printf 'case: stop-spice keeps the defaults when no ports were allocated\n'
stop_output=$(run_stop)
case $stop_output in
*':8090'*) pass 'falls back to the default HTTP port' ;;
*) fail "does not mention :8090: $stop_output" ;;
esac
case $stop_output in
*':50051'*) pass 'falls back to the default Flight port' ;;
*) fail "does not mention :50051: $stop_output" ;;
esac

printf 'case: an explicit port list still wins\n'
stop_output=$(run_stop SPICE_HTTP_PORT=21734 SPICE_FLIGHT_PORT=26734 SPICE_STOP_PORTS=9999)
case $stop_output in
*':9999'*) pass 'honours SPICE_STOP_PORTS' ;;
*) fail "does not mention :9999: $stop_output" ;;
esac
case $stop_output in
*':21734'*) fail "explicit list did not override the allocated ports: $stop_output" ;;
*) pass 'ignores the allocated ports when told which to check' ;;
esac

printf 'case: works with no Actions environment at all\n'
# Runnable by hand, and by anyone reproducing a CI failure locally.
bare_output=$(env -u RUNNER_NAME -u GITHUB_ENV -u GITHUB_RUN_ID -u GITHUB_JOB \
  -u GITHUB_RUN_ATTEMPT bash "$script" 2>&1)
bare_status=$?
assert_eq 'exits 0 with no Actions variables set' '0' "$bare_status"
case $bare_output in
*SPICE_HTTP_PORT=*) pass 'prints the assignments when GITHUB_ENV is unset' ;;
*) fail "no assignments in output: $bare_output" ;;
esac

if [ "$failures" -ne 0 ]; then
  printf '\n%s check(s) failed\n' "$failures"
  exit 1
fi

printf '\nAll checks passed\n'
