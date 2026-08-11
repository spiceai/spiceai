#!/usr/bin/env bash
# Validates the Cloud Connect `--token` bootstrap contract of deploy/chart:
#
#  - values-bootstrap.yaml renders: one replica, stateful storage, the token
#    argument as a Secret-backed env expansion, SPICE_CONFIG_DIR on the
#    volume — and never a literal enrollment key.
#  - values-connected.yaml renders with no token or Secret reference left.
#  - Template validation FAILS (before rendering) for every invalid shape:
#    more than one replica, non-persistent storage, SPICE_CONFIG_DIR missing
#    or off the volume, a literal enrollment key, or a token expansion that
#    is not backed by exactly one Kubernetes Secret environment entry.
#
# Run from the repository root: scripts/test_cloud_connect_chart.sh
# Requires: helm.

set -euo pipefail

CHART="deploy/chart"
EXAMPLES="${CHART}/examples/cloud-connect"
FAILURES=0
TEST_ENROLLMENT_KEY="spice-enroll-$(printf 'A%.0s' {1..32})"

pass() { echo "ok: $1"; }
fail() {
  echo "FAIL: $1" >&2
  FAILURES=$((FAILURES + 1))
}

render() {
  helm template cloud-connect-test "${CHART}" "$@" 2>&1
}

# --- Phase 1: the bootstrap values must render, with the key via a Secret ---
if output="$(render -f "${EXAMPLES}/values-bootstrap.yaml")"; then
  pass "values-bootstrap.yaml renders"
  echo "${output}" | grep -q -- '--token' \
    && pass "bootstrap command carries --token" \
    || fail "bootstrap command does not carry --token"
  echo "${output}" | grep -q '\$(SPICE_ENROLL_KEY)' \
    && pass "the token argument is an env expansion" \
    || fail "the token argument is not the \$(SPICE_ENROLL_KEY) expansion"
  echo "${output}" | grep -q 'secretKeyRef' \
    && pass "the key comes from a Kubernetes Secret" \
    || fail "no secretKeyRef in the bootstrap rendering"
  echo "${output}" | grep -q 'SPICE_CONFIG_DIR' \
    && pass "SPICE_CONFIG_DIR is set" \
    || fail "SPICE_CONFIG_DIR missing from the bootstrap rendering"
  echo "${output}" | grep -q 'kind: StatefulSet' \
    && pass "bootstrap renders a StatefulSet (persistent identity)" \
    || fail "bootstrap did not render a StatefulSet"
  echo "${output}" | grep -q 'failureThreshold: 66' \
    && pass "startup probe outlives the ten-minute enrollment retry budget" \
    || fail "startup probe can restart spiced before enrollment exhausts its retry budget"
  echo "${output}" | grep -q 'spice-enroll-' \
    && fail "a literal enrollment key leaked into the rendering" \
    || pass "no literal enrollment key anywhere in the rendering"
else
  fail "values-bootstrap.yaml failed to render: ${output}"
fi

# --- Phase 2: the connected values must render with every key reference gone ---
if output="$(render -f "${EXAMPLES}/values-connected.yaml")"; then
  pass "values-connected.yaml renders"
  echo "${output}" | grep -q -- '--token' \
    && fail "connected rendering still carries --token" \
    || pass "connected rendering carries no --token"
  echo "${output}" | grep -q 'SPICE_ENROLL_KEY' \
    && fail "connected rendering still references SPICE_ENROLL_KEY" \
    || pass "connected rendering has no Secret env reference"
  echo "${output}" | grep -q 'SPICE_CONFIG_DIR' \
    && pass "connected rendering keeps SPICE_CONFIG_DIR" \
    || fail "connected rendering dropped SPICE_CONFIG_DIR"
else
  fail "values-connected.yaml failed to render: ${output}"
fi

# --- Invalid shapes must fail before rendering, with the canonical message ---
expect_rejected() {
  local reason="$1"
  shift
  local output
  if output="$(render -f "${EXAMPLES}/values-bootstrap.yaml" "$@")"; then
    fail "${reason}: template rendered but must be rejected"
  elif echo "${output}" | grep -q "Cloud Connect --token"; then
    pass "${reason}: rejected with the Cloud Connect validation message"
  else
    fail "${reason}: rejected, but not by the Cloud Connect validation: ${output}"
  fi
}

expect_rejected "replicaCount above one" --set replicaCount=2
expect_rejected "non-persistent storage" --set stateful.enabled=false
expect_rejected "SPICE_CONFIG_DIR off the stateful volume" \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}},{"name":"SPICE_CONFIG_DIR","value":"/var/lib/spice"}]'
expect_rejected "SPICE_CONFIG_DIR escapes the stateful volume through traversal" \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/../var/lib/spice"}]'
expect_rejected "SPICE_CONFIG_DIR missing entirely" \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}}]'
expect_rejected "a literal enrollment key in command" \
  --set-json "command=[\"/usr/local/bin/spiced\",\"--token\",\"${TEST_ENROLLMENT_KEY}\",\"--http\",\"0.0.0.0:8090\"]"
expect_rejected "a literal enrollment key in --token= form" \
  --set-json "command=[\"/usr/local/bin/spiced\",\"--token=${TEST_ENROLLMENT_KEY}\",\"--http\",\"0.0.0.0:8090\"]"
expect_rejected "--token with no value" \
  --set-json 'command=["/usr/local/bin/spiced","--http","0.0.0.0:8090","--token"]'
expect_rejected "an undefined token environment variable" \
  --set-json 'command=["/usr/local/bin/spiced","--token","$(MISSING)","--http","0.0.0.0:8090"]'
expect_rejected "a literal token environment value" \
  --set-json "additionalEnv=[{\"name\":\"SPICE_ENROLL_KEY\",\"value\":\"${TEST_ENROLLMENT_KEY}\"},{\"name\":\"SPICE_CONFIG_DIR\",\"value\":\"/data/.spice\"}]"
expect_rejected "a token Secret reference with no key" \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"}]'
expect_rejected "duplicate token environment entries" \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}},{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"another-secret","key":"enroll-key"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"}]'

# --- A chart with no --token keeps rendering with any replica count ---
if render --set replicaCount=3 >/dev/null; then
  pass "a non-token deployment still scales freely"
else
  fail "the validation wrongly rejected a deployment without --token"
fi

# --- The example values files never hold a literal key ---
if grep -rq 'spice-enroll-[A-Za-z0-9_-]' "${EXAMPLES}"/*.yaml; then
  fail "an example values file contains a literal enrollment key"
else
  pass "example values files contain no literal enrollment key"
fi

# --- The phase transition preserves installed values and rejects timeout
#     formats its arithmetic cannot interpret before invoking cluster tools. ---
TRANSITION="${EXAMPLES}/transition-to-connected.sh"
if bash -n "${TRANSITION}"; then
  pass "transition script has valid shell syntax"
else
  fail "transition script has invalid shell syntax"
fi
if grep -q -- '--reuse-values' "${TRANSITION}"; then
  pass "transition preserves installed Helm values"
else
  fail "transition would reset installed Helm values"
fi
if output="$(SPICE_WAIT_TIMEOUT=10m bash "${TRANSITION}" test default 2>&1)"; then
  fail "transition accepted an unsupported timeout format"
elif echo "${output}" | grep -q "positive integer seconds"; then
  pass "transition rejects unsupported timeout formats before cluster access"
else
  fail "transition timeout rejection was not actionable: ${output}"
fi

echo
if [ "${FAILURES}" -gt 0 ]; then
  echo "${FAILURES} check(s) failed" >&2
  exit 1
fi
echo "all Cloud Connect chart checks passed"
