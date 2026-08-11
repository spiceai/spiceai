#!/usr/bin/env bash
# Spice Cloud Connect two-phase Helm bootstrap: the token-removal transition.
#
# Phase 1 (values-bootstrap.yaml) enrolled the instance with a one-time
# enrollment key from a Kubernetes Secret. This script performs phase 2:
#
#   1. Wait for the bootstrap pod to be Ready. The runtime is unready until
#      the enrolled identity is durable at SPICE_CONFIG_DIR on the PVC, so
#      Ready IS the durable-identity signal.
#   2. Derive a token-free override from the installed values, then upgrade —
#      removing the `--token` argument and its Secret environment reference
#      while preserving every other value — and wait for the rollout.
#   3. Verify the replacement pod establishes its control stream from the
#      stored identity alone, with no key in the pod spec.
#   4. Only then delete the exact Secret UID observed before the upgrade.
#      Deleting a Secret a pod template still references, or a same-name
#      replacement created during rollout, is forbidden.
#
# Idempotent: re-running against an already-transitioned release re-applies
# the same connected values and treats an already-deleted Secret as success.
# If another Secret has since reused the remembered name, the rerun refuses to
# delete it unless SPICE_SECRET_NAME explicitly confirms that exact name. The
# installed command and environment arrays are filtered so every custom entry
# other than the token argument and its matching env remains installed.
#
# Usage:
#   transition-to-connected.sh <release> [namespace]
#
# Environment:
#   SPICE_CHART        Chart path or name (default: deploy/chart)
#   SPICE_SECRET_NAME  Optional expected bootstrap Secret name. Normally
#                      derived from the installed secretKeyRef; on a token-free
#                      rerun, explicitly setting it confirms deletion if a
#                      Secret has reused the remembered name.
#   SPICE_WAIT_TIMEOUT Per-step wait budget as positive integer seconds
#                      with an `s` suffix (default: 600s)

set -euo pipefail

RELEASE="${1:?usage: transition-to-connected.sh <release> [namespace]}"
NAMESPACE="${2:-default}"
CHART="${SPICE_CHART:-deploy/chart}"
REQUESTED_SECRET_NAME="${SPICE_SECRET_NAME:-}"
WAIT_TIMEOUT="${SPICE_WAIT_TIMEOUT:-600s}"
case "${WAIT_TIMEOUT}" in
  *s) WAIT_SECONDS="${WAIT_TIMEOUT%s}" ;;
  *)
    echo "error: SPICE_WAIT_TIMEOUT must be positive integer seconds with an 's' suffix (for example 600s)" >&2
    exit 1
    ;;
esac
case "${WAIT_SECONDS}" in
  ''|*[!0-9]*)
    echo "error: SPICE_WAIT_TIMEOUT must be positive integer seconds with an 's' suffix (for example 600s)" >&2
    exit 1
    ;;
esac
if [ "${WAIT_SECONDS}" -eq 0 ]; then
  echo "error: SPICE_WAIT_TIMEOUT must be positive integer seconds with an 's' suffix (for example 600s)" >&2
  exit 1
fi
VALUES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELECTOR="app=${RELEASE}"
if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required to preserve installed command and environment values while removing --token" >&2
  exit 1
fi
TRANSITION_PLAN="$(mktemp "${TMPDIR:-/tmp}/spice-transition-plan.XXXXXX.json")"
CONNECTED_OVERRIDES="$(mktemp "${TMPDIR:-/tmp}/spice-connected-values.XXXXXX.json")"
WORKLOAD_JSON="$(mktemp "${TMPDIR:-/tmp}/spice-connected-workload.XXXXXX.json")"
DELETE_OPTIONS="$(mktemp "${TMPDIR:-/tmp}/spice-secret-delete.XXXXXX.json")"
trap 'rm -f -- "${TRANSITION_PLAN}" "${CONNECTED_OVERRIDES}" "${WORKLOAD_JSON}" "${DELETE_OPTIONS}"' EXIT

log() { echo "[transition-to-connected] $*"; }

workload_kind() {
  # values-bootstrap.yaml sets stateful.enabled, so the workload is normally a
  # StatefulSet; probe rather than assume so the script also serves a chart
  # whose defaults change.
  if kubectl -n "${NAMESPACE}" get statefulset -l "${SELECTOR}" -o name 2>/dev/null | grep -q .; then
    kubectl -n "${NAMESPACE}" get statefulset -l "${SELECTOR}" -o name | head -n 1
  else
    kubectl -n "${NAMESPACE}" get deployment -l "${SELECTOR}" -o name | head -n 1
  fi
}

log "step 1/4: waiting for the bootstrap pod of release '${RELEASE}' to be Ready (identity durable on the PVC)"
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l "${SELECTOR}" --timeout="${WAIT_TIMEOUT}"

if ! helm get values "${RELEASE}" --namespace "${NAMESPACE}" -o json \
  | jq -f "${VALUES_DIR}/transition-values.jq" >"${TRANSITION_PLAN}"; then
  echo "error: failed to derive token-free connected values from the installed Helm release; no upgrade was performed" >&2
  exit 1
fi
DERIVED_SECRET_NAME="$(jq -r '.bootstrapSecretName // empty' "${TRANSITION_PLAN}")"
if [ -n "${REQUESTED_SECRET_NAME}" ] && [ -n "${DERIVED_SECRET_NAME}" ] \
  && [ "${REQUESTED_SECRET_NAME}" != "${DERIVED_SECRET_NAME}" ]; then
  echo "error: SPICE_SECRET_NAME '${REQUESTED_SECRET_NAME}' does not match the installed token secretKeyRef '${DERIVED_SECRET_NAME}'; no upgrade was performed" >&2
  exit 1
fi
SECRET_NAME="${DERIVED_SECRET_NAME:-${REQUESTED_SECRET_NAME}}"
if [ -z "${SECRET_NAME}" ]; then
  echo "error: the installed release has no token Secret reference or recovery marker; set SPICE_SECRET_NAME to the exact bootstrap Secret name before retrying; no upgrade was performed" >&2
  exit 1
fi
HAD_TOKEN_REFERENCE="$(jq -r '.hadTokenReference' "${TRANSITION_PLAN}")"
DELETE_SECRET=true
case "${HAD_TOKEN_REFERENCE}" in
  true|false) ;;
  *)
    echo "error: the derived connected-values plan has no valid token-reference provenance; no upgrade was performed" >&2
    exit 1
    ;;
esac
if [ "${HAD_TOKEN_REFERENCE}" = false ] && [ -z "${REQUESTED_SECRET_NAME}" ]; then
  if ! EXISTING_SECRET="$(kubectl -n "${NAMESPACE}" get secret "${SECRET_NAME}" --ignore-not-found -o name)"; then
    echo "error: failed to verify whether the remembered Secret name '${SECRET_NAME}' currently exists; not deleting any Secret and no upgrade was performed" >&2
    exit 1
  fi
  if [ -n "${EXISTING_SECRET}" ]; then
    echo "error: the release is already token-free, so its remembered Secret name '${SECRET_NAME}' cannot authorize deletion of a currently existing Secret; set SPICE_SECRET_NAME='${SECRET_NAME}' to explicitly confirm that deletion, or remove/rename the unrelated Secret; no upgrade was performed" >&2
    exit 1
  fi
  # This recovery run found no Secret and has no explicit deletion
  # authorization. Treat absence as success and never let a same-named Secret
  # created during the rollout become a step-4 deletion target.
  DELETE_SECRET=false
fi
SECRET_UID=""
if [ "${DELETE_SECRET}" = true ]; then
  if ! SECRET_UID="$(kubectl -n "${NAMESPACE}" get secret "${SECRET_NAME}" --ignore-not-found -o 'jsonpath={.metadata.uid}')"; then
    echo "error: failed to capture the UID of bootstrap Secret '${SECRET_NAME}'; not deleting any Secret and no upgrade was performed" >&2
    exit 1
  fi
  if [ -z "${SECRET_UID}" ]; then
    # A Secret absent before the rollout cannot become a deletion target merely
    # by reusing the same name while the rollout is in progress.
    DELETE_SECRET=false
  elif ! jq -n --arg uid "${SECRET_UID}" '
    {
      apiVersion: "meta.k8s.io/v1",
      kind: "DeleteOptions",
      preconditions: {uid: $uid}
    }
  ' >"${DELETE_OPTIONS}"; then
    echo "error: failed to prepare UID-bound deletion for bootstrap Secret '${SECRET_NAME}'; no upgrade was performed" >&2
    exit 1
  fi
fi
if ! jq --arg secret "${SECRET_NAME}" \
  '.values | .cloudConnect.bootstrapSecretName = $secret' \
  "${TRANSITION_PLAN}" >"${CONNECTED_OVERRIDES}"; then
  echo "error: the derived connected-values plan is invalid; no upgrade was performed" >&2
  exit 1
fi

log "step 2/4: upgrading '${RELEASE}' with token-free installed values (removes --token and the ${SECRET_NAME} Secret reference, keeps all other values)"
helm upgrade "${RELEASE}" "${CHART}" \
  --namespace "${NAMESPACE}" \
  --reuse-values \
  -f "${CONNECTED_OVERRIDES}" \
  --wait --timeout "${WAIT_TIMEOUT}"

WORKLOAD="$(workload_kind)"
if [ -z "${WORKLOAD}" ]; then
  echo "error: no StatefulSet or Deployment found for selector '${SELECTOR}' in namespace '${NAMESPACE}'" >&2
  exit 1
fi
kubectl -n "${NAMESPACE}" rollout status "${WORKLOAD}" --timeout="${WAIT_TIMEOUT}"

# The upgraded pod template must no longer carry token syntax or an exact
# reference to the bootstrap Secret. Inspect structured fields so a release or
# volume whose name happens to contain SECRET_NAME cannot cause a false match.
if ! kubectl -n "${NAMESPACE}" get "${WORKLOAD}" -o json >"${WORKLOAD_JSON}"; then
  echo "error: failed to read the upgraded pod template; not deleting the Secret" >&2
  exit 1
fi
if jq -e --arg secret "${SECRET_NAME}" \
  -f "${VALUES_DIR}/transition-workload-clean.jq" "${WORKLOAD_JSON}" >/dev/null; then
  :
else
  jq_status=$?
  if [ "${jq_status}" -eq 1 ]; then
    echo "error: the upgraded pod template still references --token or the ${SECRET_NAME} Secret; not deleting the Secret" >&2
    exit 1
  else
    echo "error: failed to validate the upgraded pod template; not deleting the Secret" >&2
    exit 1
  fi
fi

log "step 3/4: verifying the replacement pod reconnects from the stored identity"
deadline=$(( $(date +%s) + WAIT_SECONDS ))
until kubectl -n "${NAMESPACE}" logs -l "${SELECTOR}" --tail=-1 2>/dev/null \
  | grep -q "Cloud Connect: stream established"; do
  if [ "$(date +%s)" -ge "${deadline}" ]; then
    echo "error: the replacement pod did not establish its Cloud Connect stream within ${WAIT_TIMEOUT}; not deleting the Secret" >&2
    echo "hint: check 'kubectl -n ${NAMESPACE} logs -l ${SELECTOR}' — the identity should reconnect with no --token" >&2
    exit 1
  fi
  sleep 2
done
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l "${SELECTOR}" --timeout="${WAIT_TIMEOUT}"

if [ "${DELETE_SECRET}" = true ]; then
  log "step 4/4: deleting the single-use bootstrap Secret '${SECRET_NAME}' observed before the upgrade"
  SECRET_API_PATH="/api/v1/namespaces/${NAMESPACE}/secrets/${SECRET_NAME}"
  if kubectl delete --raw "${SECRET_API_PATH}" -f "${DELETE_OPTIONS}" >/dev/null 2>&1; then
    :
  else
    if ! CURRENT_SECRET_UID="$(kubectl -n "${NAMESPACE}" get secret "${SECRET_NAME}" --ignore-not-found -o 'jsonpath={.metadata.uid}')"; then
      echo "error: UID-bound deletion of bootstrap Secret '${SECRET_NAME}' failed, and its current UID could not be verified; the Secret was preserved" >&2
      exit 1
    fi
    if [ -z "${CURRENT_SECRET_UID}" ]; then
      log "step 4/4: bootstrap Secret '${SECRET_NAME}' was already absent"
    elif [ "${CURRENT_SECRET_UID}" != "${SECRET_UID}" ]; then
      echo "error: bootstrap Secret '${SECRET_NAME}' changed from UID '${SECRET_UID}' to '${CURRENT_SECRET_UID}' during the rollout; the replacement Secret was preserved" >&2
      exit 1
    else
      echo "error: failed to delete bootstrap Secret '${SECRET_NAME}' with its captured UID '${SECRET_UID}'; the Secret was preserved" >&2
      exit 1
    fi
  fi
else
  log "step 4/4: bootstrap Secret '${SECRET_NAME}' was already absent; no deletion was authorized or attempted"
fi

log "done: '${RELEASE}' runs from its stored identity; preserve its connected installed values with --reuse-values or a maintained custom values file on future upgrades"
