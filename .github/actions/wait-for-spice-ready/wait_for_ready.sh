#!/usr/bin/env bash
#
# Wait for the Spice runtime to report ready, and say what went wrong when it
# does not.
#
# What this replaces, repeated ~14 times across the E2E workflow:
#
#   while [[ "$(curl -s http://localhost:8090/v1/ready)" != "ready" ]]; do sleep 1; done
#
# with `timeout-minutes` around the step. That loop prints nothing, so a
# timeout produced exactly one line — "The action ... has timed out after 5
# minutes" — for every distinct cause: a runtime that could not bind its port,
# a model that failed to download, a dataset stuck loading, a `spiced` that
# died on startup. #12058 asked for the next occurrence to carry a real
# diagnosis; this is that diagnosis.
#
# Exits non-zero on timeout, after dumping the evidence.

set -uo pipefail

URL="${SPICE_READY_URL:-http://localhost:8090/v1/ready}"
TIMEOUT="${SPICE_READY_TIMEOUT:-300}"
INTERVAL="${SPICE_READY_INTERVAL:-2}"
# How often to say something while waiting. A silent 5-minute step is
# indistinguishable from a hung runner in the Actions UI.
PROGRESS_EVERY="${SPICE_READY_PROGRESS_EVERY:-15}"
LOG_FILE="${SPICE_READY_LOG:-spice.log}"

port_from_url() {
  # http://localhost:8090/v1/ready -> 8090; defaults to 80 when absent.
  local hostport="${URL#*://}"
  hostport="${hostport%%/*}"
  case "$hostport" in
    *:*) printf '%s' "${hostport##*:}" ;;
    *)   printf '80' ;;
  esac
}

# Runs only on the failure path, and is deliberately not itself time-bounded:
# `lsof` can block on a wedged mount, and the job-level `timeout-minutes` is the
# backstop for that. A step-level timeout is not an option — the inner steps of
# a composite action do not honour one — and wrapping this in `timeout(1)` is
# not portable to the macOS runners, which do not ship it.
diagnose() {
  local last_body="$1" port
  port="$(port_from_url)"

  echo "::group::Diagnosis: runtime never reported ready"

  echo "--- last response from ${URL} ---"
  if [ -z "$last_body" ]; then
    echo "(empty — nothing was listening, or the request failed)"
  else
    printf '%s\n' "$last_body"
  fi

  echo "--- spice / spiced processes ---"
  spice_pids="$(pgrep -x spice 2>/dev/null || true)"
  spiced_pids="$(pgrep -x spiced 2>/dev/null || true)"
  if [ -n "$spice_pids" ] || [ -n "$spiced_pids" ]; then
    # Alive but not ready is a startup problem; the log below says which.
    # These are machine-wide: on a shared runner some may belong to another
    # runner instance, which is itself worth seeing here.
    [ -n "$spice_pids" ] && echo "spice:  ${spice_pids}"
    [ -n "$spiced_pids" ] && echo "spiced: ${spiced_pids}"
  else
    # Nothing alive means the runtime exited — the log holds its dying words.
    echo "(none — the runtime is not running; it exited or never started)"
  fi

  echo "--- listeners on :${port} ---"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null || echo "(none)"
  elif command -v fuser >/dev/null 2>&1; then
    fuser "${port}/tcp" 2>/dev/null || echo "(none)"
  else
    echo "(no lsof/fuser on this runner)"
  fi

  echo "--- ${LOG_FILE} ---"
  if [ -f "$LOG_FILE" ]; then
    cat "$LOG_FILE"
    # The single highest-value line: a port already held means a previous job on
    # this runner leaked its runtime, not that this job is broken.
    if grep -q "Address already in use" "$LOG_FILE" 2>/dev/null; then
      echo
      echo "NOTE: the runtime could not bind its port because something else already held it."
      echo "      Two causes, and the process list above tells them apart:"
      echo "        - an orphaned 'spiced' left by an earlier job on this runner; or"
      echo "        - a concurrent job, since several runner instances share one machine"
      echo "          and therefore one network namespace."
      echo "      See .github/actions/stop-spice and #12058."
    fi
  else
    echo "(no ${LOG_FILE} in $(pwd))"
  fi

  echo "::endgroup::"
}

echo "Waiting up to ${TIMEOUT}s for ${URL} to report ready…"

start=$SECONDS
last_body=""
next_progress=$PROGRESS_EVERY

while :; do
  # No `-f`. A runtime that is up but not ready answers 503 with a body saying
  # so (crates/runtime/src/http/v1/ready.rs), and `-f` suppresses the body on an
  # error status — which would leave every progress line and the timeout
  # diagnosis reporting an empty response, the exact blindness being fixed here.
  # The exact `= "ready"` comparison below is what guards against a false pass.
  # `|| true`: curl still exits non-zero while nothing is listening yet, which
  # is the normal state early on and must not end the wait.
  last_body="$(curl -sS --max-time 5 "$URL" 2>/dev/null || true)"

  if [ "$last_body" = "ready" ]; then
    # "Something on :8090 says ready" is not "our runtime is ready". Several
    # runner instances share one machine and one network namespace, so if our
    # `spiced` failed to bind, this poll can be answered by a *different job's*
    # runtime — and every test step after this would then run against it.
    # Failing to bind always leaves this line in our own log, so treat it as
    # authoritative over whatever answered the socket.
    if [ -f "$LOG_FILE" ] && grep -q "Address already in use" "$LOG_FILE" 2>/dev/null; then
      echo "Something on ${URL} reports ready, but it is not this job's runtime:"
      echo "${LOG_FILE} shows our own spiced failed to bind its port."
      diagnose "$last_body"
      exit 1
    fi
    echo "Runtime ready after $((SECONDS - start))s."
    exit 0
  fi

  elapsed=$((SECONDS - start))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Runtime did not report ready within ${TIMEOUT}s."
    diagnose "$last_body"
    exit 1
  fi

  if [ "$elapsed" -ge "$next_progress" ]; then
    # Echo what it actually said, not just that it is not "ready" — a runtime
    # answering "initializing" is a very different problem from a dead socket.
    echo "  ${elapsed}s — not ready yet (last response: '${last_body}')"
    next_progress=$((elapsed + PROGRESS_EVERY))
  fi

  sleep "$INTERVAL"
done
