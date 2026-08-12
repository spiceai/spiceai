#!/usr/bin/env bash
# Validates the Cloud Connect `--token` bootstrap contract of deploy/chart:
#
#  - values-bootstrap.yaml renders: one replica, stateful storage, the token
#    argument as a Secret-backed env expansion, SPICE_CONFIG_DIR on the
#    volume — and never a literal enrollment key.
#  - values-connected.yaml renders with no token or Secret reference left.
#  - Template validation FAILS (before rendering) for every invalid shape or
#    unsupported implicit/computed bootstrap command:
#    more than one replica, non-persistent storage, SPICE_CONFIG_DIR missing
#    or off the volume, a literal enrollment key, or a token expansion that
#    is not backed by exactly one Kubernetes Secret environment entry.
#
# Run from the repository root: scripts/test_cloud_connect_chart.sh
# Requires: helm and jq.

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
  echo "${output}" | grep -q 'failureThreshold: 67' \
    && pass "startup probe outlives the ten-minute enrollment retry budget" \
    || fail "startup probe can restart spiced before enrollment exhausts its retry budget"
  echo "${output}" | sed -n '/^[[:space:]]*startupProbe:$/,/^[[:space:]]*volumeMounts:$/p' | grep -q 'path: /health' \
    && pass "startup probe remains enrollment-dependent" \
    || fail "startup probe does not use the local enrollment-dependent health endpoint"
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
if output="$(render -f "${EXAMPLES}/values-connected.yaml" --set replicaCount=0)"; then
  fail "connected mode rendered with zero replicas"
elif echo "${output}" | grep -q "Direct Cloud Connect requires one replica"; then
  pass "connected mode rejects zero replicas"
else
  fail "connected mode rejected zero replicas for the wrong reason: ${output}"
fi

# --- Invalid shapes must fail before rendering, with the canonical message ---
expect_rejected() {
  local reason="$1"
  shift
  local output
  if output="$(render -f "${EXAMPLES}/values-bootstrap.yaml" "$@")"; then
    fail "${reason}: template rendered but must be rejected"
  elif echo "${output}" | grep -Eq "Cloud Connect|cloudConnect.mode"; then
    pass "${reason}: rejected with the Cloud Connect validation message"
  else
    fail "${reason}: rejected, but not by the Cloud Connect validation: ${output}"
  fi
}

expect_rejected "replicaCount above one" --set replicaCount=2
expect_rejected "replicaCount zero" --set replicaCount=0
expect_rejected "non-persistent storage" --set stateful.enabled=false
expect_rejected "startup probe shorter than the enrollment retry window" \
  --set startupProbe.failureThreshold=60
expect_rejected "startup probe whose final failure occurs before 660 seconds" \
  --set startupProbe.failureThreshold=66
expect_rejected "an exec startup probe that can succeed before enrollment" \
  --set-json 'startupProbe.exec={"command":["true"]}'
expect_rejected "a TCP startup probe that bypasses the required health check" \
  --set-json 'startupProbe.tcpSocket={"port":8090}'
expect_rejected "a gRPC startup probe that bypasses the required health check" \
  --set-json 'startupProbe.grpc={"port":8090}'
expect_rejected "a remote startup probe that can succeed before enrollment" \
  --set startupProbe.httpGet.host=example.invalid
expect_rejected "a startup probe on the wrong health path" \
  --set startupProbe.httpGet.path=/v1/ready
expect_rejected "a startup probe on the wrong HTTP port" \
  --set startupProbe.httpGet.port=8091
expect_rejected "an HTTPS startup probe that cannot observe the HTTP listener" \
  --set startupProbe.httpGet.scheme=HTTPS
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
expect_rejected "a quoted shell-form token command" \
  --set-json 'command=["/bin/sh","-c","spiced '\''--token'\'' $(SPICE_ENROLL_KEY)"]'
expect_rejected "a backslash-escaped shell-form token command" \
  --set-json 'command=["/bin/sh","-c","spiced \\--token $(SPICE_ENROLL_KEY)"]'
expect_rejected "a shell-spliced backslash token command" \
  --set-json 'command=["/bin/sh","-c","spiced --to\\ken $(SPICE_ENROLL_KEY)"]'
expect_rejected "a shell-spliced quoted token command" \
  --set-json 'command=["/bin/sh","-c","spiced --to\"\"ken $(SPICE_ENROLL_KEY)"]'
expect_rejected "a shell-variable-computed token command" \
  --set-json 'command=["/bin/sh","-c","T=--token; exec spiced $T $SPICE_ENROLL_KEY"]'
expect_rejected "a command-substitution-computed token command" \
  --set-json 'command=["/bin/sh","-c","exec spiced $(printf %s --token)=$SPICE_ENROLL_KEY"]'
expect_rejected "an encoded-computed token command" \
  --set-json 'command=["/bin/sh","-c","exec spiced $(echo LS10b2tlbg== | base64 -d) $SPICE_ENROLL_KEY"]'
expect_rejected "an ANSI-C-quoted shell-form token command" \
  --set-json 'command=["/bin/bash","-c","spiced $'\''--token'\'' $(SPICE_ENROLL_KEY)"]'
expect_rejected "a quote-adjacent shell-form token command" \
  --set-json 'command=["/bin/sh","-c","spiced '\''--token'\''$(SPICE_ENROLL_KEY)"]'
expect_rejected "mixed direct and shell-form token commands" \
  --set-json 'command=["/bin/sh","-c","spiced --token literal","--token","$(SPICE_ENROLL_KEY)"]'
expect_rejected "a shell argv-zero token that is not passed to spiced" \
  --set-json 'command=["/bin/sh","-c","exec spiced","--token","$(SPICE_ENROLL_KEY)"]'
expect_rejected "--token after the end-of-options marker" \
  --set-json 'command=["/usr/local/bin/spiced","--","--token","$(SPICE_ENROLL_KEY)"]'
expect_rejected "--token while Cloud Connect mode is disabled" \
  --set cloudConnect.mode=disabled
expect_rejected "--token while Cloud Connect mode is connected" \
  --set cloudConnect.mode=connected
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

# A custom bootstrap that does not load the example inherits the chart's
# ordinary 30-second startup probe and must be rejected before rendering.
if output="$(render \
  --set cloudConnect.mode=bootstrap \
  --set stateful.enabled=true \
  --set-json 'command=["spiced","--token","$(SPICE_ENROLL_KEY)"]' \
  --set-json 'additionalEnv=[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"custom-enroll","key":"token"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"}]')"; then
  fail "a custom token bootstrap rendered with the chart's 30-second startup probe"
elif echo "${output}" | grep -q "requires at least 660 seconds"; then
  pass "custom token bootstraps reject the chart's short default startup probe"
else
  fail "custom token bootstrap failed for the wrong reason: ${output}"
fi

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

# --- Docker bootstrap must not inherit its steady-state restart loop ---
COMPOSE_BOOTSTRAP="deploy/docker/cloud-connect/compose.bootstrap.yaml"
if grep -Eq '^[[:space:]]+restart: "no"$' "${COMPOSE_BOOTSTRAP}"; then
  pass "Docker bootstrap disables automatic restarts"
else
  fail "Docker bootstrap inherits the steady-state automatic restart policy"
fi
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  if merged_restart="$(SPICE_ENROLL_KEY="${TEST_ENROLLMENT_KEY}" docker compose \
    -f deploy/docker/cloud-connect/compose.yaml \
    -f "${COMPOSE_BOOTSTRAP}" config --format json | jq -r '.services.spiced.restart')" \
    && [ "${merged_restart}" = "no" ]; then
    pass "Docker Compose merge applies the bootstrap restart override"
  else
    fail "Docker Compose merge kept restart policy ${merged_restart:-unknown} during bootstrap"
  fi
else
  pass "Docker Compose merge check skipped because the CLI is unavailable"
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
installed_values='{"command":["/usr/local/bin/spiced","--token","$(SPICE_ENROLL_KEY)","--http","0.0.0.0:8090","--set-runtime","custom.mode=enabled"],"additionalEnv":[{"name":"SPICE_ENROLL_KEY","valueFrom":{"secretKeyRef":{"name":"spice-cloud-connect","key":"enroll-key"}}},{"name":"SPICE_CONFIG_DIR","value":"/data/.spice"},{"name":"CUSTOM_FLAG","value":"kept"}],"cloudConnect":{"mode":"bootstrap"}}'
if transition_plan="$(printf '%s' "${installed_values}" | jq -f "${TRANSITION_FILTER}")"; then
  connected_values="$(printf '%s' "${transition_plan}" | jq '.values')"
  if printf '%s' "${connected_values}" | jq -e '
    (.command | index("--token") == null)
    and (.command | index("$(SPICE_ENROLL_KEY)") == null)
    and (.command | index("custom.mode=enabled") != null)
    and (.additionalEnv | map(.name) | index("SPICE_ENROLL_KEY") == null)
    and (.additionalEnv | map(.name) | index("SPICE_CONFIG_DIR") != null)
    and (.additionalEnv | map(.name) | index("CUSTOM_FLAG") != null)
    and (.cloudConnect.mode == "connected")
  ' >/dev/null; then
    pass "transition removes only the token argument and matching env"
  else
    fail "transition dropped a custom command or environment value: ${connected_values}"
  fi
  if printf '%s' "${transition_plan}" | jq -e '
    .bootstrapSecretName == "spice-cloud-connect"
    and .hadTokenReference == true
  ' >/dev/null; then
    pass "transition derives the exact installed Secret name"
  else
    fail "transition did not derive the installed Secret name: ${transition_plan}"
  fi
  second_plan="$(printf '%s' "${connected_values}" | jq -f "${TRANSITION_FILTER}")"
  second_values="$(printf '%s' "${second_plan}" | jq '.values')"
  if [ "$(printf '%s' "${connected_values}" | jq -S -c .)" = "$(printf '%s' "${second_values}" | jq -S -c .)" ] \
    && printf '%s' "${second_plan}" | jq -e '
      .bootstrapSecretName == "spice-cloud-connect"
      and .hadTokenReference == false
    ' >/dev/null; then
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
quoted_shell_token_values='{"command":["/bin/sh","-c","spiced '\''--token'\'' $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${quoted_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted quoted shell-form token syntax"
else
  pass "transition rejects quoted shell-form token syntax before upgrade"
fi
escaped_shell_token_values='{"command":["/bin/sh","-c","spiced \\--token $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${escaped_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted backslash-escaped shell-form token syntax"
else
  pass "transition rejects backslash-escaped shell-form token syntax before upgrade"
fi
spliced_escaped_shell_token_values='{"command":["/bin/sh","-c","spiced --to\\ken $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${spliced_escaped_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted a shell-spliced backslash token option"
else
  pass "transition rejects shell-spliced backslash token syntax before upgrade"
fi
spliced_quoted_shell_token_values='{"command":["/bin/sh","-c","spiced --to\"\"ken $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${spliced_quoted_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted a shell-spliced quoted token option"
else
  pass "transition rejects shell-spliced quoted token syntax before upgrade"
fi
ansi_quoted_shell_token_values='{"command":["/bin/bash","-c","spiced $'\''--token'\'' $(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${ansi_quoted_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted ANSI-C-quoted shell-form token syntax"
else
  pass "transition rejects ANSI-C-quoted shell-form token syntax before upgrade"
fi
adjacent_quoted_shell_token_values='{"command":["/bin/sh","-c","spiced '\''--token'\''$(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${adjacent_quoted_shell_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted quote-adjacent shell-form token syntax"
else
  pass "transition rejects quote-adjacent shell-form token syntax before upgrade"
fi
mixed_token_values='{"command":["/bin/sh","-c","spiced --token literal","--token","$(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if printf '%s' "${mixed_token_values}" | jq -f "${TRANSITION_FILTER}" >/dev/null 2>&1; then
  fail "transition accepted mixed direct and shell-form token syntax"
else
  pass "transition rejects mixed direct and shell-form token syntax before upgrade"
fi
shell_argv_zero_values='{"command":["/bin/sh","-c","exec spiced","--token","$(ENROLLMENT)"],"additionalEnv":[{"name":"ENROLLMENT","valueFrom":{"secretKeyRef":{"name":"secret","key":"token"}}}]}'
if output="$(printf '%s' "${shell_argv_zero_values}" | jq -e -f "${TRANSITION_FILTER}" 2>&1)"; then
  fail "transition accepted a shell argv-zero token that is not passed to spiced"
elif echo "${output}" | grep -q "not attached to a direct spiced command"; then
  pass "transition rejects a shell argv-zero token before upgrade"
else
  fail "transition rejected a shell argv-zero token for the wrong reason: ${output}"
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
secret_bearing_workload='{"spec":{"template":{"spec":{"imagePullSecrets":[{"name":"pull-secret"}],"containers":[{"command":["spiced"],"envFrom":[{"secretRef":{"name":"envfrom-secret"}}]}],"initContainers":[{"name":"init","env":[{"name":"INIT","valueFrom":{"secretKeyRef":{"name":"init-secret","key":"value"}}}]}],"volumes":[{"name":"direct","secret":{"secretName":"volume-secret"}},{"name":"projected","projected":{"sources":[{"secret":{"name":"projected-secret"}}]}},{"name":"csi","csi":{"driver":"secrets-store.csi.k8s.io","nodePublishSecretRef":{"name":"csi-secret"}}}]}}}}'
for referenced_secret in pull-secret envfrom-secret init-secret volume-secret projected-secret csi-secret; do
  if printf '%s' "${secret_bearing_workload}" \
    | jq -e --arg secret "${referenced_secret}" -f "${WORKLOAD_FILTER}" >/dev/null; then
    fail "structured workload validation missed ${referenced_secret}"
  else
    pass "structured workload validation detects ${referenced_secret}"
  fi
done
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
quoted_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/sh","-c","spiced '\''--token'\'' $(KEY)"]}]}}}}'
if printf '%s' "${quoted_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed quoted token syntax"
else
  pass "structured workload validation detects quoted token syntax"
fi
escaped_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/sh","-c","spiced \\--token $(KEY)"]}]}}}}'
if printf '%s' "${escaped_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed backslash-escaped token syntax"
else
  pass "structured workload validation detects backslash-escaped token syntax"
fi
spliced_escaped_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/sh","-c","spiced --to\\ken $(KEY)"]}]}}}}'
if printf '%s' "${spliced_escaped_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed shell-spliced backslash token syntax"
else
  pass "structured workload validation detects shell-spliced backslash token syntax"
fi
spliced_quoted_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/sh","-c","spiced --to\"\"ken $(KEY)"]}]}}}}'
if printf '%s' "${spliced_quoted_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed shell-spliced quoted token syntax"
else
  pass "structured workload validation detects shell-spliced quoted token syntax"
fi
ansi_quoted_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/bash","-c","spiced $'\''--token'\'' $(KEY)"]}]}}}}'
if printf '%s' "${ansi_quoted_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed ANSI-C-quoted token syntax"
else
  pass "structured workload validation detects ANSI-C-quoted token syntax"
fi
adjacent_quoted_token_workload='{"spec":{"template":{"spec":{"containers":[{"command":["/bin/sh","-c","spiced '\''--token'\''$(KEY)"]}]}}}}'
if printf '%s' "${adjacent_quoted_token_workload}" \
  | jq -e --arg secret spice-cloud-connect -f "${WORKLOAD_FILTER}" >/dev/null; then
  fail "structured workload validation missed quote-adjacent token syntax"
else
  pass "structured workload validation detects quote-adjacent token syntax"
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
TRANSITION_TEST_INSTALLED_VALUES="${connected_values}"
kubectl() {
  if printf '%s\n' "$*" | grep -q ' wait '; then
    return 0
  fi
  if printf '%s\n' "$*" | grep -q ' get secret spice-cloud-connect --ignore-not-found -o name'; then
    printf '%s\n' 'secret/spice-cloud-connect'
    return 0
  fi
  return 99
}
export -f kubectl
if output="$(bash "${TRANSITION}" test default 2>&1)"; then
  fail "a token-free rerun deleted a Secret using only its remembered name"
elif echo "${output}" | grep -q "remembered Secret name 'spice-cloud-connect' cannot authorize deletion"; then
  pass "a token-free recovery marker cannot delete a newly same-named Secret"
else
  fail "token-free Secret provenance failed for the wrong reason: ${output}"
fi
kubectl() {
  if printf '%s\n' "$*" | grep -q ' wait '; then
    return 0
  fi
  return 99
}
export -f kubectl
if output="$(bash "${TRANSITION}" test default 2>&1)"; then
  fail "a token-free rerun continued when Secret provenance could not be checked"
elif echo "${output}" | grep -q "failed to verify whether the remembered Secret name 'spice-cloud-connect' currently exists"; then
  pass "a failed Secret provenance lookup prevents upgrade and deletion"
else
  fail "failed Secret provenance lookup was not fail-closed: ${output}"
fi
helm() {
  if [ "${1:-} ${2:-}" = "get values" ]; then
    printf '%s\n' "${TRANSITION_TEST_INSTALLED_VALUES}"
    return 0
  elif [ "${1:-}" = "upgrade" ]; then
    return 0
  fi
  return 99
}
kubectl() {
  arguments="$*"
  if printf '%s\n' "${arguments}" | grep -q ' get secret spice-cloud-connect --ignore-not-found -o name'; then
    # Absent at the provenance check. If the script later calls delete, the
    # mock models a newly-created same-name Secret and fails loudly.
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' delete secret spice-cloud-connect '; then
    echo 'MOCK_UNAUTHORIZED_DELETE_REACHED' >&2
    return 99
  fi
  if printf '%s\n' "${arguments}" | grep -q ' get statefulset/test -o json'; then
    printf '%s\n' '{"spec":{"template":{"spec":{"containers":[{"command":["spiced","--http","0.0.0.0:8090"]}]}}}}'
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' get statefulset -l app=test -o name'; then
    printf '%s\n' 'statefulset/test'
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' logs -l app=test '; then
    printf '%s\n' 'Cloud Connect: stream established'
    # Keep writing after the early match. With `set -o pipefail`, a `grep -q`
    # consumer closes the pipe and makes this producer fail with SIGPIPE.
    seq 1 100000
    return $?
  fi
  if printf '%s\n' "${arguments}" | grep -q ' wait \| rollout status '; then
    return 0
  fi
  return 99
}
export -f helm kubectl
TRANSITION_TEST_INSTALLED_VALUES="${connected_values}"
if output="$(SPICE_WAIT_TIMEOUT=2s bash "${TRANSITION}" test default 2>&1)" \
  && ! echo "${output}" | grep -q 'MOCK_UNAUTHORIZED_DELETE_REACHED' \
  && echo "${output}" | grep -q "was already absent; no deletion was authorized"; then
  pass "stream detection consumes the full log pipe under pipefail"
  pass "a token-free absent-Secret rerun cannot delete a same-name Secret created during rollout"
else
  fail "an absent token-free Secret remained a deletion target: ${output}"
fi
helm() {
  if [ "${1:-} ${2:-}" = "get values" ]; then
    printf '%s\n' "${TRANSITION_TEST_INSTALLED_VALUES}"
  elif [ "${1:-}" = "upgrade" ]; then
    previous=''
    for argument in "$@"; do
      if [ "${previous}" = '-f' ] \
        && jq -e '.cloudConnect.bootstrapSecretName == "spice-cloud-connect"' "${argument}" >/dev/null; then
        echo "MOCK_RECOVERY_MARKER_PERSISTED" >&2
      fi
      previous="${argument}"
    done
    return 99
  else
    return 99
  fi
}
kubectl() {
  if printf '%s\n' "$*" | grep -q ' wait '; then
    return 0
  fi
  if printf '%s\n' "$*" | grep -q ' get secret spice-cloud-connect --ignore-not-found -o name'; then
    printf '%s\n' 'secret/spice-cloud-connect'
    return 0
  fi
  if printf '%s\n' "$*" | grep -q ' get secret spice-cloud-connect --ignore-not-found -o jsonpath='; then
    printf '%s\n' 'uid-explicitly-confirmed'
    return 0
  fi
  return 99
}
export -f helm kubectl
TRANSITION_TEST_INSTALLED_VALUES='{"command":["spiced","--http","0.0.0.0:8090"],"additionalEnv":[]}'
if output="$(SPICE_SECRET_NAME=spice-cloud-connect bash "${TRANSITION}" test default 2>&1)"; then
  fail "the explicit Secret confirmation mock unexpectedly completed the transition"
elif echo "${output}" | grep -q 'MOCK_RECOVERY_MARKER_PERSISTED' \
  && ! echo "${output}" | grep -q 'cannot authorize deletion'; then
  pass "an exact SPICE_SECRET_NAME authorizes a token-free rerun and persists its recovery marker"
else
  fail "an exact SPICE_SECRET_NAME did not persist its recovery marker: ${output}"
fi

# Capture the original Secret UID before a token-bearing upgrade and submit it
# as a server-side delete precondition. A same-name replacement that appears
# during rollout must receive the API's conflict rather than being deleted.
UID_RACE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/spice-uid-race.XXXXXX")"
UID_QUERY_STATE="${UID_RACE_DIR}/uid-read"
export UID_QUERY_STATE
helm() {
  if [ "${1:-} ${2:-}" = "get values" ]; then
    printf '%s\n' "${TRANSITION_TEST_INSTALLED_VALUES}"
  elif [ "${1:-}" = "upgrade" ]; then
    return 0
  else
    return 99
  fi
}
kubectl() {
  arguments="$*"
  if printf '%s\n' "${arguments}" | grep -q ' get secret spice-cloud-connect --ignore-not-found -o jsonpath='; then
    if [ -e "${UID_QUERY_STATE}" ]; then
      printf '%s\n' 'uid-replacement'
    else
      : >"${UID_QUERY_STATE}"
      printf '%s\n' 'uid-original'
    fi
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q '^delete --raw /api/v1/namespaces/default/secrets/spice-cloud-connect -f '; then
    previous=''
    for argument in "$@"; do
      if [ "${previous}" = '-f' ]; then
        if jq -e '.preconditions.uid == "uid-original"' "${argument}" >/dev/null; then
          return 1
        fi
        return 0
      fi
      previous="${argument}"
    done
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' get statefulset/test -o json'; then
    printf '%s\n' '{"spec":{"template":{"spec":{"containers":[{"command":["spiced","--http","0.0.0.0:8090"]}]}}}}'
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' get statefulset -l app=test -o name'; then
    printf '%s\n' 'statefulset/test'
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' logs -l app=test '; then
    printf '%s\n' 'Cloud Connect: stream established'
    return 0
  fi
  if printf '%s\n' "${arguments}" | grep -q ' wait \| rollout status '; then
    return 0
  fi
  return 99
}
export -f helm kubectl
TRANSITION_TEST_INSTALLED_VALUES="${installed_values}"
if output="$(bash "${TRANSITION}" test default 2>&1)"; then
  fail "the UID-precondition race mock deleted a same-name replacement"
elif echo "${output}" | grep -q "changed from UID 'uid-original' to 'uid-replacement'"; then
  pass "the initial transition preserves a same-name replacement with a Secret UID precondition"
else
  fail "the initial transition did not fail safely on Secret UID replacement: ${output}"
fi
rm -f -- "${UID_QUERY_STATE}"
rmdir -- "${UID_RACE_DIR}"
unset UID_QUERY_STATE
unset -f helm kubectl
unset TRANSITION_TEST_INSTALLED_VALUES

echo
if [ "${FAILURES}" -gt 0 ]; then
  echo "${FAILURES} check(s) failed" >&2
  exit 1
fi
echo "all Cloud Connect chart checks passed"
