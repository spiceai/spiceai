#!/usr/bin/env bash
#
# Stop the Spice runtime this job started, and report whether the ports it used
# came free.
#
# Two facts about the E2E runners drive everything here.
#
# 1. `spice run` does not become the runtime. The CLI spawns `spiced` as a child
#    (`get_run_cmd` in bin/spice/src/context.rs builds `Command::new(spiced_path)`),
#    and `spiced` is the process that binds the HTTP and Flight ports. So
#    `killall spice` — what every E2E cleanup used to do — reaps the CLI and
#    orphans the runtime still holding :8090. The next job to land on that
#    runner then dies with
#
#      ERROR spiced: Unable to start Spice Runtime servers: Unable to start HTTP
#      server: Unable to bind to address: Address already in use (os error 48)
#
# 2. Several runner instances share one physical machine. A runner is named
#    `<machine>-NN` and works out of `/opt/github-runner-NN/_work`, so
#    `spiceai-macos-runner-04` hosts `-01`, `-02`, `-03`… concurrently, and they
#    share one network namespace. That makes a name-wide `killall spiced` worse
#    than the leak it fixes: it would reap a *concurrent* job's runtime on the
#    same host.
#
# So this stops by PID, and only PIDs whose working directory lives inside this
# runner instance's workspace. Another instance's runtime is left strictly alone
# — and if it is the one holding the port, that is reported, because a port held
# after our own processes are gone is the shared-machine collision rather than a
# leak (see #12058).
#
# Exits 0 even when nothing was running: cleanup runs under `if: always()` and
# must never be the reason a job fails.

set -uo pipefail

# Ports `spiced` binds (HTTP, Flight). Left unquoted where used so the
# space-separated value splits into words.
SPICE_STOP_PORTS="${SPICE_STOP_PORTS:-8090 50051}"

# Seconds to wait for a graceful exit before escalating to SIGKILL, and then for
# the ports to clear. Kept short: this runs in cleanup on every job.
GRACE="${SPICE_STOP_GRACE:-10}"
PORT_WAIT="${SPICE_STOP_PORT_WAIT:-15}"

# Only processes rooted inside this path are ours. `RUNNER_WORKSPACE` is set by
# the Actions runner and is unique per instance. Empty means "no scope known" —
# a developer running this by hand — in which case every spice/spiced is fair
# game, which is what someone running it locally means by "stop spice".
SCOPE="${SPICE_STOP_SCOPE:-${RUNNER_WORKSPACE:-}}"

# A process's reported cwd is fully resolved, so a scope containing a symlink
# would never match it and we would silently skip our own runtime. Compare
# against the resolved form too. Falls back to the raw value when the path does
# not exist or there is no way to resolve it.
SCOPE_RESOLVED="$SCOPE"
if [ -n "$SCOPE" ] && [ -d "$SCOPE" ]; then
  SCOPE_RESOLVED="$(cd "$SCOPE" 2>/dev/null && pwd -P)" || SCOPE_RESOLVED="$SCOPE"
  [ -z "$SCOPE_RESOLVED" ] && SCOPE_RESOLVED="$SCOPE"
fi

# Both names, every time. `-x`/`killall` match exactly, so `spice` never matches
# `spiced` and vice versa — the omission this script exists to fix.
PROCS=(spice spiced)

# A process's working directory, via /proc on Linux or lsof on macOS. Empty when
# it cannot be determined.
proc_cwd() {
  local pid="$1" cwd=""
  if [ -e "/proc/${pid}/cwd" ]; then
    cwd="$(readlink "/proc/${pid}/cwd" 2>/dev/null || true)"
  fi
  if [ -z "$cwd" ] && command -v lsof >/dev/null 2>&1; then
    cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -1)"
  fi
  printf '%s' "$cwd"
}

# Refuses on an undeterminable cwd. Leaking a runtime costs the next job a
# retry; killing a concurrent job's runtime destroys work already in flight, so
# when in doubt this does nothing.
in_scope() {
  local pid="$1" cwd
  [ -z "$SCOPE" ] && return 0
  cwd="$(proc_cwd "$pid")"
  if [ -z "$cwd" ]; then
    # stderr, not stdout: the caller runs this inside a command substitution
    # collecting PIDs, so anything on stdout is read back as a PID. Only on the
    # first pass — this is re-enumerated once a second while waiting.
    [ "${WARN_UNKNOWN:-0}" = "1" ] &&
      echo "stop-spice: cannot determine cwd of PID ${pid} — leaving it alone." >&2
    return 1
  fi
  case "$cwd" in
    "$SCOPE" | "$SCOPE"/* | "$SCOPE_RESOLVED" | "$SCOPE_RESOLVED"/*) return 0 ;;
    *) return 1 ;;
  esac
}

# PIDs of our spice/spiced processes, space separated.
our_pids() {
  local name pid out=""
  for name in "${PROCS[@]}"; do
    for pid in $(pgrep -x "$name" 2>/dev/null || true); do
      if in_scope "$pid"; then
        out="${out:+${out} }${pid}"
      fi
    done
  done
  printf '%s' "$out"
}

# Whether any spice/spiced exists at all, in scope or not. Kept separate from
# our_pids because that runs in a command substitution, so a variable it sets
# would not survive back to the caller.
any_running() {
  local name
  for name in "${PROCS[@]}"; do
    pgrep -x "$name" >/dev/null 2>&1 && return 0
  done
  return 1
}

# PIDs listening on a port, via whichever tool the runner has. Prints the
# sentinel `unknown` when neither exists, so "no tool" is not read as "free".
port_holders() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null || true
  elif command -v fuser >/dev/null 2>&1; then
    fuser "${port}/tcp" 2>/dev/null || true
  else
    printf 'unknown'
  fi
}

if [ -z "$SCOPE" ]; then
  echo "stop-spice: no RUNNER_WORKSPACE — stopping every spice/spiced on this host."
else
  echo "stop-spice: stopping spice/spiced under ${SCOPE}."
fi

WARN_UNKNOWN=1
pids="$(our_pids)"
WARN_UNKNOWN=0
if [ -z "$pids" ]; then
  if any_running; then
    # Worth saying out loud rather than exiting quietly: on a shared machine
    # this is the normal, correct outcome (another instance's job is running).
    # But it is also what a mis-set scope looks like, and a scope that never
    # matches would silently restore the leak this script exists to prevent.
    echo "stop-spice: spice/spiced is running, but none of it is under ${SCOPE:-/} — leaving it alone."
  else
    echo "stop-spice: no spice/spiced process of ours is running."
  fi
else
  # shellcheck disable=SC2086 # deliberate word splitting of the PID list
  echo "stop-spice: sending SIGTERM to PID(s): ${pids}"
  # shellcheck disable=SC2086
  kill -TERM ${pids} 2>/dev/null || true

  deadline=$((SECONDS + GRACE))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ -z "$(our_pids)" ] && break
    sleep 1
  done

  remaining="$(our_pids)"
  if [ -z "$remaining" ]; then
    echo "stop-spice: exited cleanly."
  else
    # A runtime that ignores SIGTERM is either wedged or mid-shutdown; either
    # way the next job inherits the port if it is left running.
    echo "stop-spice: PID(s) ${remaining} still alive after ${GRACE}s — sending SIGKILL."
    # shellcheck disable=SC2086
    kill -KILL ${remaining} 2>/dev/null || true
    sleep 1
    still="$(our_pids)"
    [ -n "$still" ] && echo "stop-spice: warning: PID(s) ${still} survived SIGKILL."
  fi
fi

# The process table clearing is not the same as the socket clearing: a killed
# listener can sit in TIME_WAIT briefly, and on this shared machine the port may
# belong to a different runner instance entirely. Distinguish those two.
# shellcheck disable=SC2086 # deliberate word splitting of the port list
for port in ${SPICE_STOP_PORTS}; do
  deadline=$((SECONDS + PORT_WAIT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    holders="$(port_holders "$port")"
    if [ -z "$holders" ] || [ "$holders" = "unknown" ]; then
      break
    fi
    sleep 1
  done

  holders="$(port_holders "$port")"
  if [ "$holders" = "unknown" ]; then
    echo "stop-spice: no lsof/fuser on this runner — cannot confirm :${port} is free."
  elif [ -n "$holders" ]; then
    # Our processes are gone by now, so whatever holds this port is not ours.
    # On a machine running several runner instances that is a concurrent job,
    # and it means this port is contended by design rather than leaked.
    echo "stop-spice: note: :${port} is still held by PID(s) ${holders}, none of them ours."
    echo "stop-spice: on a shared runner machine that is another runner instance's job, not a leak."
  else
    echo "stop-spice: :${port} is free."
  fi
done

exit 0
