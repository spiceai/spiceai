#!/usr/bin/env bash
# Starts the Databricks cluster that the Spark Connect integration tests use, and waits for it.
#
#   databricks_cluster.sh start   Requests a start if the cluster is stopped, then returns.
#   databricks_cluster.sh wait    Waits until the cluster runs, requesting a start if it is stopped.
#
# A stopped cluster takes about 7 minutes to start. The tests fail on the first `PENDING`
# answer, so CI requests the start before the test archive builds and waits before the tests run.
#
# Needs DATABRICKS_HOST and DATABRICKS_CLUSTER_ID, plus either DATABRICKS_TOKEN or what
# databricks_oidc_token.sh needs to exchange the job's GitHub OIDC token.
# DATABRICKS_CLUSTER_WAIT_SECONDS bounds `wait` (default 720); DATABRICKS_CLUSTER_POLL_SECONDS
# sets its poll interval (default 15).
set -euo pipefail
# A conditional turns `set -e` off for everything it calls, and bash before 4.4 has no
# `inherit_errexit`, so the functions below return their failures explicitly.
shopt -s inherit_errexit 2>/dev/null || true

: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_CLUSTER_ID:?DATABRICKS_CLUSTER_ID is required}"
wait_seconds="${DATABRICKS_CLUSTER_WAIT_SECONDS:-720}"
poll_seconds="${DATABRICKS_CLUSTER_POLL_SECONDS:-15}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
api="https://${DATABRICKS_HOST}/api/2.1/clusters"

# A token from the OIDC exchange expires after 5 minutes, so every request gets a new one.
token() {
  if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
    printf '%s' "${DATABRICKS_TOKEN}"
  else
    bash "${script_dir}/databricks_oidc_token.sh"
  fi
}

# Prints the response body. On an HTTP error, logs the response and fails; the caller decides
# whether that error fails the step.
api_call() {
  local endpoint="${*: -1}" bearer body
  bearer="$(token)" || return 1
  if ! body="$(curl --silent --show-error --fail-with-body --max-time 30 --retry 3 --retry-all-errors \
    --header "Authorization: Bearer ${bearer}" "$@")"; then
    echo "Databricks request ${endpoint##*/} for cluster ${DATABRICKS_CLUSTER_ID} failed: ${body}" >&2
    return 1
  fi
  printf '%s' "${body}"
}

# Sets `state` and `message` from the cluster's current state.
read_state() {
  local response
  if ! response="$(api_call --get --data-urlencode "cluster_id=${DATABRICKS_CLUSTER_ID}" "${api}/get")"; then
    echo "::error::Could not read the state of cluster ${DATABRICKS_CLUSTER_ID}. The response is above." >&2
    return 1
  fi
  if ! state="$(jq -er '.state | strings' <<<"${response}")"; then
    echo "::error::Databricks returned no state for cluster ${DATABRICKS_CLUSTER_ID}: ${response}" >&2
    return 1
  fi
  message="$(jq -r '.state_message // ""' <<<"${response}")" || return 1
}

describe() {
  printf 'Cluster %s is %s%s' "${DATABRICKS_CLUSTER_ID}" "${state}" "${message:+ (${message})}"
}

# Requests a start of a stopped cluster. The test partitions wait concurrently, so another job
# may start the cluster after this one read `TERMINATED`, and Databricks then rejects this
# request. That is not a failure while the cluster is starting or running.
request_start() {
  local body
  body="$(jq -cn --arg id "${DATABRICKS_CLUSTER_ID}" '{cluster_id: $id}')" || return 1
  if api_call --header 'Content-Type: application/json' --data "${body}" "${api}/start" >/dev/null; then
    echo "Requested a start of cluster ${DATABRICKS_CLUSTER_ID}."
    return 0
  fi
  read_state || return 1
  case "${state}" in
    PENDING | RESTARTING | RUNNING | RESIZING)
      echo "$(describe); another job started it."
      return 0
      ;;
  esac
  echo "::error::Could not start cluster ${DATABRICKS_CLUSTER_ID}, which is ${state}${message:+ (${message})}. The response is above." >&2
  return 1
}

unexpected_state() {
  echo "::error::$(describe), which this script does not handle. Check the cluster's event log in Databricks." >&2
  exit 1
}

start() {
  read_state
  echo "$(describe)."
  case "${state}" in
    TERMINATED) request_start ;;
    # A terminating cluster cannot be started yet; `wait` starts it once it has stopped.
    PENDING | RUNNING | RESIZING | RESTARTING | TERMINATING) ;;
    *) unexpected_state ;;
  esac
}

wait_until_running() {
  local deadline=$((SECONDS + wait_seconds)) started=false last=""
  while :; do
    read_state
    if [[ "$(describe)" != "${last}" ]]; then
      last="$(describe)"
      echo "$(date -u +%H:%M:%S) ${last}."
    fi
    case "${state}" in
      RUNNING | RESIZING) return 0 ;;
      TERMINATED)
        # A cluster that stops again after this script started it failed to start; the
        # message carries Databricks' termination reason.
        if [[ "${started}" == true ]]; then
          echo "::error::$(describe) after a requested start. Check the cluster's event log in Databricks." >&2
          exit 1
        fi
        request_start
        started=true
        ;;
      PENDING | RESTARTING | TERMINATING) ;;
      *) unexpected_state ;;
    esac
    if ((SECONDS >= deadline)); then
      echo "::error::$(describe) after ${wait_seconds}s of waiting for RUNNING. Check the cluster's event log in Databricks." >&2
      exit 1
    fi
    sleep "${poll_seconds}"
  done
}

case "${1:-}" in
  start) start ;;
  wait) wait_until_running ;;
  *)
    echo "usage: $0 start|wait" >&2
    exit 2
    ;;
esac
