#!/usr/bin/env bash
# Spice Cloud Connect two-phase Helm bootstrap: the token-removal transition.
#
# Phase 1 (values-bootstrap.yaml) enrolled the instance with a one-time
# enrollment key from a Kubernetes Secret. This script performs phase 2:
#
#   1. Wait for the bootstrap pod to be Ready. The runtime is unready until
#      the enrolled identity is durable at SPICE_CONFIG_DIR on the PVC, so
#      Ready IS the durable-identity signal.
#   2. Upgrade the release to values-connected.yaml — removing the `--token`
#      argument and its Secret environment reference while keeping
#      SPICE_CONFIG_DIR and the stateful volume — and wait for the rollout.
#   3. Verify the replacement pod establishes its control stream from the
#      stored identity alone, with no key in the pod spec.
#   4. Only then delete the Secret. Deleting a Secret a pod template still
#      references is forbidden — that ordering is the point of this script.
#
# Idempotent: re-running against an already-transitioned release re-applies
# the same connected values and treats an already-deleted Secret as success.
#
# Usage:
#   transition-to-connected.sh <release> [namespace]
#
# Environment:
#   SPICE_CHART        Chart path or name (default: deploy/chart)
#   SPICE_SECRET_NAME  Bootstrap Secret name (default: spice-cloud-connect)
#   SPICE_WAIT_TIMEOUT Per-step wait budget as positive integer seconds
#                      with an `s` suffix (default: 600s)

set -euo pipefail

RELEASE="${1:?usage: transition-to-connected.sh <release> [namespace]}"
NAMESPACE="${2:-default}"
CHART="${SPICE_CHART:-deploy/chart}"
SECRET_NAME="${SPICE_SECRET_NAME:-spice-cloud-connect}"
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

log "step 2/4: upgrading '${RELEASE}' to the connected values (removes --token and the ${SECRET_NAME} Secret reference, keeps the volume)"
helm upgrade "${RELEASE}" "${CHART}" \
  --namespace "${NAMESPACE}" \
  --reuse-values \
  -f "${VALUES_DIR}/values-connected.yaml" \
  --wait --timeout "${WAIT_TIMEOUT}"

WORKLOAD="$(workload_kind)"
if [ -z "${WORKLOAD}" ]; then
  echo "error: no StatefulSet or Deployment found for selector '${SELECTOR}' in namespace '${NAMESPACE}'" >&2
  exit 1
fi
kubectl -n "${NAMESPACE}" rollout status "${WORKLOAD}" --timeout="${WAIT_TIMEOUT}"

# The upgraded pod spec must no longer reference the token or the Secret.
if kubectl -n "${NAMESPACE}" get "${WORKLOAD}" -o yaml | grep -qE -- "--token|${SECRET_NAME}"; then
  echo "error: the upgraded pod template still references --token or the ${SECRET_NAME} Secret; not deleting the Secret" >&2
  exit 1
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

log "step 4/4: deleting the single-use bootstrap Secret '${SECRET_NAME}'"
kubectl -n "${NAMESPACE}" delete secret "${SECRET_NAME}" --ignore-not-found

log "done: '${RELEASE}' runs from its stored identity; use values-connected.yaml for all future upgrades"
