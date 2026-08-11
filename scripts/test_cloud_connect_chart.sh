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
expect_rejected "duplicate SPICE_CONFIG_DIR entries" \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"},{"name":"SPICE_CONFIG_DIR","value":"/var/lib/spice"}]'
expect_rejected "a literal enrollment key in command" \
  --set-json "command=[\"/usr/local/bin/spiced\",\"--token\",\"${TEST_ENROLLMENT_KEY}\",\"--http\",\"0.0.0.0:8090\"]"
expect_rejected "a literal enrollment key in --token= form" \
  --set-json "command=[\"/usr/local/bin/spiced\",\"--token=${TEST_ENROLLMENT_KEY}\",\"--http\",\"0.0.0.0:8090\"]"
expect_rejected "a shell-form token command" \
  --set-json "command=[\"/bin/sh\",\"-c\",\"spiced --token ${TEST_ENROLLMENT_KEY}\"]"
expect_rejected "an env-wrapped shell-form token command" \
  --set-json "command=[\"/usr/bin/env\",\"sh\",\"-c\",\"spiced --token ${TEST_ENROLLMENT_KEY}\"]"
expect_rejected "mixed direct and shell-form token commands" \
  --set-json 'command=["/bin/sh","-c","spiced --token literal","--token","$(SPICE_ENROLL_KEY)"]'
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
if render --set-json 'command=["spiced","--set-runtime","debug.flag=--token"]' >/dev/null; then
  pass "incidental token-like text in a direct argument is not bootstrap syntax"
else
  fail "the validation mistook incidental direct-argument text for token syntax"
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
if sed -n '/helm upgrade/,/--wait --timeout/p' "${TRANSITION}" | grep -q 'values-connected.yaml'; then
  fail "transition would overwrite customized scalar or stateful values from the static example"
else
  pass "transition does not apply mount-bearing static example values"
fi
TRANSITION_FILTER="${EXAMPLES}/transition-values.jq"
installed_values='{"command":["/usr/local/bin/spiced","--token","$(SPICE_ENROLL_KEY)","--http","0.0.0.0:8090","--set-runtime","custom.mode=enabled"],"additionalEnv":[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"},{"name":"CUSTOM_FLAG","value":"kept"}]}'
if transition_plan="$(printf '%s' "${installed_values}" | jq -f "${TRANSITION_FILTER}")"; then
  connected_values="$(printf '%s' "${transition_plan}" | jq '.values')"
  if printf '%s' "${connected_values}" | jq -e '
    (.command | index("--token") == null)
    and (.command | index("$(SPICE_ENROLL_KEY)") == null)
    and (.command | index("custom.mode=enabled") != null)
    and (.additionalEnv | map(.name) | index("SPICE_ENROLL_KEY") == null)
    and (.additionalEnv | map(.name) | index("SPICE_CONFIG_DIR") != null)
    and (.additionalEnv | map(.name) | index("CUSTOM_FLAG") != null)
  ' >/dev/null; then
    pass "transition removes only the token argument and matching env"
  else
    fail "transition dropped a custom command or environment value: ${connected_values}"
  fi
  if printf '%s' "${transition_plan}" | jq -e '.bootstrapSecretName == "spice-cloud-connect"' >/dev/null; then
    pass "transition derives the exact installed Secret name"
  else
    fail "transition did not derive the installed Secret name: ${transition_plan}"
  fi
  second_plan="$(printf '%s' "${connected_values}" | jq -f "${TRANSITION_FILTER}")"
  second_values="$(printf '%s' "${second_plan}" | jq '.values')"
  if [ "$(printf '%s' "${connected_values}" | jq -S -c .)" = "$(printf '%s' "${second_values}" | jq -S -c .)" ] \
    && printf '%s' "${second_plan}" | jq -e '.bootstrapSecretName == "spice-cloud-connect"' >/dev/null; then
    pass "connected-value filtering is idempotent"
  else
    fail "connected-value filtering changed an already-connected release or forgot its Secret: ${second_plan}"
  fi
else
  fail "transition could not derive connected values from a valid bootstrap release"
fi
duplicate_token_values='{"command":["spiced","--token","$(FIRST)","--token=$(SECOND)"],"additionalEnv":[]}'
if printf '%s' "${duplicate_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted an ambiguous installed command with two tokens"
else
  pass "transition rejects ambiguous installed token commands"
fi
equals_token_values='{"command":["spiced","--token=$(ENROLLMENT)","--http","127.0.0.1:8090"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"custom-enroll-secret","key":"token"}}},{"name":"CUSTOM","value":"kept"}]}'
if equals_plan="$(printf '%s' "${equals_token_values}" | jq -f "${TRANSITION_FILTER}")" \
  && printf '%s' "${equals_plan}" | jq -e '
    (.bootstrapSecretName == "custom-enroll-secret")
    and (.values.command | all(startswith("--token=") | not))
    and (.values.additionalEnv | map(.name) == ["CUSTOM"])
  ' >/dev/null; then
  pass "transition removes --token= form and preserves its unrelated env"
else
  fail "transition mishandled the --token= form: ${equals_plan:-no plan}"
fi
shell_token_values='{"command":["/bin/sh","-c","spiced --token $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted unsupported shell-form token syntax"
else
  pass "transition rejects installed shell-form token syntax before upgrade"
fi
wrapped_shell_token_values='{"command":["/usr/bin/env","sh","-c","spiced --token $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${wrapped_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted env-wrapped shell-form token syntax"
else
  pass "transition rejects env-wrapped shell-form token syntax before upgrade"
fi
mixed_token_values='{"command":["/bin/sh","-c","spiced --token literal","--token","$(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${mixed_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted mixed direct and shell-form token syntax"
else
  pass "transition rejects mixed direct and shell-form token syntax before upgrade"
fi
stale_marker_values='{"command":["spiced","--token","$(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"installed-secret","key":"token"}}}],"cloudConnect":{"bootstrapSecretName":"different-secret"}}'
if printf '%s' "${stale_marker_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted a remembered Secret name that conflicts with the installed secretKeyRef"
else
  pass "transition rejects conflicting remembered and installed Secret names"
fi
incidental_token_values='{"command":["spiced","--set-runtime","debug.flag=--token"],"additionalEnv":[{"name":"CUSTOM","value":"kept"}]}'
if incidental_plan="$(printf '%s' "${incidental_token_values}" | jq -f "${TRANSITION_FILTER}")" \
  && printf '%s' "${incidental_plan}" | jq -e '
    (.values.command == ["spiced", "--set-runtime", "debug.flag=--token"])
    and (.values.additionalEnv == [{"name":"CUSTOM","value":"kept"}])
  ' >/dev/null; then
  pass "incidental token-like text in a direct argument is preserved"
else
  fail "transition treated incidental direct-argument text as token syntax: ${incidental_plan:-no plan}"
fi
WORKLOAD_FILTER="${EXAMPLES}/transition-workload-clean.jq"
name_collision_workload='{"metadata":{"name":"spice-cloud-connect","labels":{"app":"spice-cloud-connect"}},"spec":{"template":{"spec":{"containers":[{"command":["spiced","--http","0.0.0.0:8090"],"env":[{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"}]}],"volumes":[{"name":"spice-cloud-connect-data"}]}}}}'
if printf '%s' "${name_collision_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  pass "release-name collisions do not impersonate Secret references"
else
  fail "structured workload validation rejected an unrelated name collision"
fi
secret_ref_workload='{"spec":{"template":{"spec":{"containers":[{"command":["spiced"],"env":[{"name":"OTHER","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"value"}}}]}]}}}}'
if printf '%s' "${secret_ref_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed an exact bootstrap Secret reference"
else
  pass "structured workload validation detects the exact Secret reference"
fi
token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/sh","-c","spiced --token $(KEY)"]}]}}}}'
if printf '%s' "${token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed residual shell-form token syntax"
else
  pass "structured workload validation detects residual token syntax"
fi
wrapped_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/usr/bin/env","sh","-c","spiced --token $(KEY)"]}]}}}}'
if printf '%s' "${wrapped_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed residual env-wrapped token syntax"
else
  pass "structured workload validation detects env-wrapped token syntax"
fi
incidental_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["spiced","--set-runtime","debug.flag=--token"]}]}}}}'
if printf '%s' "${incidental_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  pass "structured workload validation preserves incidental direct-argument text"
else
  fail "structured workload validation mistook incidental text for a token argument"
fi
if output="$(SPICE_WAIT_TIMEOUT=10m bash "${TRANSITION}" test default 2>&1)"; then
  fail "transition accepted an unsupported timeout format"
elif echo "${output}" | grep -q "positive integer seconds"; then
  pass "transition rejects unsupported timeout formats before cluster access"
else
  fail "transition timeout rejection was not actionable: ${output}"
fi

# The shell-level expected-Secret guard is exercised with exported mocks so it
# must fail after reading installed values but before any upgrade or deletion.
TRANSITION_TEST_INSTALLED_VALUES="${installed_values}"
export TRANSITION_TEST_INSTALLED_VALUES
helm() {
  if [ "${1:-} ${2:-}" = "get values" ]; then
    printf '%s\n' "${TRANSITION_TEST_INSTALLED_VALUES}"
  else
    return 99
  fi
}
kubectl() {
  if printf '%s\n' "$*" | grep -q ' wait '; then
    return 0
  fi
  return 99
}
export -f helm kubectl
if output="$(SPICE_SECRET_NAME=different-secret bash "${TRANSITION}" test default 2>&1)"; then
  fail "transition accepted SPICE_SECRET_NAME that conflicts with installed values"
elif echo "${output}" | grep -q "does not match the installed token secretKeyRef"; then
  pass "transition rejects a mismatched SPICE_SECRET_NAME before upgrade"
else
  fail "transition did not report the expected Secret-name mismatch: ${output}"
fi
TRANSITION_TEST_INSTALLED_VALUES='{"command":["spiced","--http","0.0.0.0:8090"],"additionalEnv":[]}'
if output="$(bash "${TRANSITION}" test default 2>&1)"; then
  fail "transition guessed a Secret name for an unmarked token-free release"
elif echo "${output}" | grep -q "set SPICE_SECRET_NAME to the exact bootstrap Secret name"; then
  pass "transition refuses to guess a Secret for an unmarked token-free release"
else
  fail "transition did not fail safely for an unmarked token-free release: ${output}"
fi
unset -f helm kubectl
unset TRANSITION_TEST_INSTALLED_VALUES

echo
if [ "${FAILURES}" -gt 0 ]; then
  echo "${FAILURES} check(s) failed" >&2
  exit 1
fi
echo "all Cloud Connect chart checks passed"
