#!/usr/bin/env bash
#
# Pick the HTTP and Flight ports this job's Spice runtime should bind, and
# export them for every later step.
#
# Why a job needs its own ports at all: the self-hosted pools run several runner
# instances per physical host (`/opt/github-runner-01`, `-02`, `-03`, … on one
# machine), and those instances share a network namespace. Every `spice run`
# binds spiced's defaults, 127.0.0.1:8090 and 127.0.0.1:50051, so two jobs that
# overlap on one host cannot both bind. The second exits, and the readiness poll
# then waits out its whole timeout with nothing to show for it. Worse, if the
# process already holding 8090 is a *ready* runtime from the other job, the poll
# succeeds and the tests run against a spicepod they did not load. See #12419.
#
# The port is derived from the runner instance rather than from the run or the
# job, because the runner instance is the unit that shares the host: one
# instance runs one job at a time, so distinct instances holding distinct ports
# is exactly the property that makes the collision impossible. Run and job are
# folded in as well so that a port leaked by an earlier job on this same
# instance is unlikely to be the one we ask for next.
#
# Outputs, written to $GITHUB_ENV:
#   SPICE_HTTP_PORT       e.g. 21734
#   SPICE_FLIGHT_PORT     e.g. 26734
#   SPICE_HTTP_ENDPOINT   e.g. http://127.0.0.1:21734
#   SPICE_FLIGHT_ENDPOINT e.g. 127.0.0.1:26734   (bare host:port — `spiced --flight` takes no scheme)

set -uo pipefail

# Both windows sit below the ephemeral range on every runner OS — Linux
# allocates from 32768 and macOS from 49152 — so a port picked here cannot be
# taken from under us by an unrelated outbound connection. They are also far
# from spiced's own defaults, which keeps a stray 8090 in a log unambiguous:
# that is a step that has not been given the job's port.
HTTP_BASE="${SPICE_PORT_HTTP_BASE:-20000}"
FLIGHT_OFFSET="${SPICE_PORT_FLIGHT_OFFSET:-5000}"
SPAN="${SPICE_PORT_SPAN:-4000}"
# How far to walk forward when the derived port is already held. Small on
# purpose: a busy derived port means a leak or a hash collision, and walking a
# long way would only hide it.
MAX_PROBE="${SPICE_PORT_MAX_PROBE:-25}"

# Seed. RUNNER_NAME is the one that matters — it identifies the runner instance,
# and therefore the collision domain. The others only spread repeat jobs on one
# instance apart. All are optional so the script is runnable outside Actions.
seed_source="${RUNNER_NAME:-}|${GITHUB_RUN_ID:-}|${GITHUB_JOB:-}|${GITHUB_RUN_ATTEMPT:-}"

# `cksum` rather than a shell hash or `md5`: it is POSIX, present on both runner
# families, and its first field is already a decimal integer.
seed_hash() {
  printf '%s' "$1" | cksum | awk '{print $1}'
}

# True when something is already listening on the port. Tries each tool the
# runners might have and, if none is present, answers "not in use" — a probe we
# cannot run must not block the job, and binding is still checked for real by
# spiced a moment later.
port_in_use() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi

  if command -v netstat >/dev/null 2>&1; then
    # `.port` is the macOS/BSD separator, `:port` the Linux one.
    netstat -an 2>/dev/null | grep -E "[.:]${port}[[:space:]]+.*LISTEN" >/dev/null 2>&1
    return $?
  fi

  return 1
}

hash_value="$(seed_hash "${seed_source}")"
if [ -z "${hash_value}" ]; then
  # cksum is missing or produced nothing. $$ is unique among live processes on
  # the host, which is the same property the seed was after.
  hash_value=$$
fi

offset=$((hash_value % SPAN))

# Walk both ports together so the pair keeps a constant distance; that way one
# number in a log identifies the other, and neither walks into the other's
# window. The step wraps within SPAN rather than running past it, so a seed that
# lands near the top of the window cannot walk out of the range this script
# promises — and into the ephemeral range it was chosen to avoid.
probe=0
while :; do
  http_port=$((HTTP_BASE + (offset + probe) % SPAN))
  flight_port=$((http_port + FLIGHT_OFFSET))

  if ! port_in_use "${http_port}" && ! port_in_use "${flight_port}"; then
    break
  fi

  probe=$((probe + 1))
  if [ "${probe}" -ge "${MAX_PROBE}" ]; then
    break
  fi

  echo "Port ${http_port}/${flight_port} is already held on this host; trying the next pair."
done

if [ "${probe}" -ge "${MAX_PROBE}" ]; then
  # Report rather than fail: spiced will refuse to bind and say so, and the
  # readiness wait turns that into a named failure. Failing here instead would
  # replace a specific diagnosis with a vaguer one.
  echo "::warning::Could not find a free port pair within ${MAX_PROBE} tries of ${HTTP_BASE}+${offset}; using ${http_port}/${flight_port} anyway."
fi

http_endpoint="http://127.0.0.1:${http_port}"
# No scheme: `spice run --flight-endpoint` hands this to `spiced --flight`
# verbatim, and that flag takes a bind address.
flight_endpoint="127.0.0.1:${flight_port}"

echo "Spice runtime ports for this job: HTTP ${http_port}, Flight ${flight_port} (runner '${RUNNER_NAME:-unknown}')."

assignments="SPICE_HTTP_PORT=${http_port}
SPICE_FLIGHT_PORT=${flight_port}
SPICE_HTTP_ENDPOINT=${http_endpoint}
SPICE_FLIGHT_ENDPOINT=${flight_endpoint}"

# Always logged: whoever reads a failed job needs the port to make sense of the
# readiness poll and of stop-spice's report. Outside Actions this is also the
# only output, so the script stays runnable by hand.
printf '%s\n' "${assignments}"

if [ -n "${GITHUB_ENV:-}" ]; then
  printf '%s\n' "${assignments}" >>"${GITHUB_ENV}"
fi
