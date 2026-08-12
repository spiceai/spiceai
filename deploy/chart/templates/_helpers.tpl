{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "spiceai.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "spiceai.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "spiceai.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "spiceai.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Allow the release namespace to be overridden for multi-namespace deployments in combined charts
*/}}
{{- define "spiceai.namespace" -}}
  {{- if .Values.namespaceOverride -}}
    {{- .Values.namespaceOverride -}}
  {{- else -}}
    {{- .Release.Namespace -}}
  {{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "spiceai.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Generate basic labels
*/}}
{{- define "spiceai.labels" }}
helm.sh/chart: {{ template "spiceai.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: controller
app.kubernetes.io/part-of: {{ template "spiceai.name" . }}
{{- include "spiceai.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.Version | quote }}
{{- if .Values.additionalLabels }}
{{ toYaml .Values.additionalLabels }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "spiceai.selectorLabels" }}
{{- if .Values.selectorOverride }}
{{ toYaml .Values.selectorOverride }}
{{- else }}
app.kubernetes.io/name: {{ include "spiceai.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- end }}

{{/*
Canonical boundary detector for direct shell syntax and option names assembled
with shell-removed quoting, escaping, or empty expansion. Keep the same
expression in the two transition jq filters under
deploy/chart/examples/cloud-connect/.
*/}}
{{- define "spiceai.cloudConnectTokenSyntaxPattern" -}}
(^|[[:space:];|&])[$`'"\\]*-[$`'"\\]*-[$`'"\\]*t[$`'"\\]*o[$`'"\\]*k[$`'"\\]*e[$`'"\\]*n[`'"\\]*($|[=$[:space:];|&])
{{- end -}}

{{/*
Whether the container command carries Cloud Connect `--token` syntax. Exact
two-element (`--token`, `$(VAR)`) and single-element (`--token=$(VAR)`) forms
are validated below; token syntax embedded in any other argument is detected
here and rejected there. The boundary excludes values such as
`debug.flag=--token`, which are not separate CLI arguments.
*/}}
{{- define "spiceai.cloudConnectTokenBootstrap" -}}
{{- $found := false -}}
{{- $tokenSyntaxPattern := include "spiceai.cloudConnectTokenSyntaxPattern" . -}}
{{- range .Values.command -}}
{{- if or (eq . "--token") (hasPrefix "--token=" .) (regexMatch $tokenSyntaxPattern .) -}}
{{- $found = true -}}
{{- end -}}
{{- end -}}
{{- if $found }}true{{ end -}}
{{- end -}}

{{/*
Validate direct Cloud Connect deployment modes before rendering.

One enrollment key enrolls exactly one identity, and the identity must
survive a pod replacement, so `cloudConnect.mode: bootstrap` requires:
  - a direct `spiced` executable followed by exactly one command-array
    `--token` argument (arbitrary shell programs are intentionally rejected),
  - exactly one replica,
  - stateful persistent storage,
  - SPICE_CONFIG_DIR set to a literal path beneath the stateful mountPath,
  - exactly one token argument expanding `$(VAR)`, with exactly one matching
    `additionalEnv` entry backed by a non-empty Secret name and key — never a
    literal key baked into a chart value or environment value.

The `connected` mode retains the single-replica persistent-identity contract
and rejects a leftover direct token. `disabled` is the default for ordinary
deployments. Explicit mode is required because templates cannot safely infer
security policy by interpreting arbitrary shell programs or entrypoints.
*/}}
{{- define "spiceai.validateCloudConnect" -}}
{{- $cloudConnect := .Values.cloudConnect | default dict -}}
{{- $mode := get $cloudConnect "mode" | default "disabled" -}}
{{- if not (has $mode (list "disabled" "bootstrap" "connected")) -}}
{{- fail "cloudConnect.mode must be one of disabled, bootstrap, or connected. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- if regexMatch "spice-enroll-[A-Za-z0-9_-]{32}([^A-Za-z0-9_-]|$)" (toJson .Values) -}}
{{- fail "Cloud Connect enrollment keys must come from a Kubernetes Secret and must never be stored in chart values. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $hasTokenSyntax := include "spiceai.cloudConnectTokenBootstrap" . -}}
{{- if and (eq $mode "disabled") $hasTokenSyntax -}}
{{- fail "Cloud Connect --token is permitted only with cloudConnect.mode: bootstrap. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- if and (eq $mode "connected") $hasTokenSyntax -}}
{{- fail "cloudConnect.mode: connected must not retain --token syntax. Run the Cloud Connect transition before deleting the bootstrap Secret. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- if eq $mode "bootstrap" -}}
{{- if eq (len .Values.command) 0 -}}
{{- fail "cloudConnect.mode: bootstrap requires a direct spiced command array; shell entrypoints and implicit image commands cannot prove which executable receives --token. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $commandExecutable := first .Values.command -}}
{{- if ne (base $commandExecutable) "spiced" -}}
{{- fail "cloudConnect.mode: bootstrap requires a direct spiced command array; shell entrypoints and implicit image commands cannot prove which executable receives --token. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $hasDirectTokenArg := false -}}
{{- $hasUnsupportedTokenSyntax := false -}}
{{- $afterEndOfOptions := false -}}
{{- $tokenSyntaxPattern := include "spiceai.cloudConnectTokenSyntaxPattern" . -}}
{{- range .Values.command -}}
{{- if eq . "--" -}}
{{- $afterEndOfOptions = true -}}
{{- else if or (eq . "--token") (hasPrefix "--token=" .) -}}
{{- $hasDirectTokenArg = true -}}
{{- if $afterEndOfOptions -}}
{{- $hasUnsupportedTokenSyntax = true -}}
{{- end -}}
{{- else if regexMatch $tokenSyntaxPattern . -}}
{{- $hasUnsupportedTokenSyntax = true -}}
{{- end -}}
{{- end -}}
{{- if or (not $hasDirectTokenArg) $hasUnsupportedTokenSyntax -}}
{{- fail "cloudConnect.mode: bootstrap requires --token as a direct command-array option before any -- end-of-options marker; shell-form, computed, or embedded token syntax is unsupported. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- end -}}
{{- if or (eq $mode "bootstrap") (eq $mode "connected") -}}
{{- if or (ne (int .Values.replicaCount) 1) (not .Values.stateful.enabled) -}}
{{- fail "Direct Cloud Connect requires one replica and persistent Spice identity storage. Set replicaCount: 1 and stateful.enabled: true, and set SPICE_CONFIG_DIR beneath stateful.mountPath. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $mount := clean (.Values.stateful.mountPath | default "") -}}
{{- $configDirCount := 0 -}}
{{- $configDirOk := false -}}
{{- range .Values.additionalEnv -}}
{{- if eq (get . "name") "SPICE_CONFIG_DIR" -}}
{{- $configDirCount = add1 $configDirCount -}}
{{- if get . "value" -}}
{{- $configDir := clean (get . "value") -}}
{{- if and (ne $mount ".") (hasPrefix (printf "%s/" $mount) $configDir) -}}
{{- $configDirOk = true -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if or (ne $configDirCount 1) (not $configDirOk) -}}
{{- fail "Direct Cloud Connect requires one replica and persistent Spice identity storage. Add exactly one additionalEnv entry setting SPICE_CONFIG_DIR to a path beneath stateful.mountPath (for example /data/.spice under mountPath /data). See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- end -}}
{{- if eq $mode "bootstrap" -}}
{{- $startupProbe := .Values.startupProbe | default dict -}}
{{- $startupHttpGet := (get $startupProbe "httpGet") | default dict -}}
{{- $startupHttpScheme := upper (get $startupHttpGet "scheme" | default "HTTP") -}}
{{- if or (get $startupProbe "exec") (get $startupProbe "tcpSocket") (get $startupProbe "grpc") (ne (get $startupHttpGet "path") "/health") (ne (toString (get $startupHttpGet "port")) "8090") (get $startupHttpGet "host") (ne $startupHttpScheme "HTTP") -}}
{{- fail "Cloud Connect --token bootstrap requires the local HTTP startup probe at /health on port 8090; exec, TCP, gRPC, HTTPS, and remote-host probes can succeed too early or never observe runtime health. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $startupPeriodSeconds := int (get $startupProbe "periodSeconds" | default 10) -}}
{{- $startupFailureThreshold := int (get $startupProbe "failureThreshold" | default 3) -}}
{{- $startupInitialDelaySeconds := int (get $startupProbe "initialDelaySeconds" | default 0) -}}
{{- $startupBudgetSeconds := add $startupInitialDelaySeconds (mul $startupPeriodSeconds (sub $startupFailureThreshold 1)) -}}
{{- if lt $startupBudgetSeconds 660 -}}
{{- fail "Cloud Connect --token bootstrap requires at least 660 seconds before the failure-threshold probe can restart spiced, so its ten-minute enrollment retry window can finish. Increase startupProbe.failureThreshold, periodSeconds, or initialDelaySeconds. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $cmd := .Values.command -}}
{{- $tokenArgCount := 0 -}}
{{- $tokenEnvName := "" -}}
{{- range $i, $arg := $cmd -}}
{{- if eq $arg "--token" -}}
{{- $tokenArgCount = add1 $tokenArgCount -}}
{{- $next := "" -}}
{{- if lt (add1 $i) (len $cmd) -}}
{{- $next = index $cmd (add1 $i) -}}
{{- end -}}
{{- if not (regexMatch "^\\$\\([A-Za-z_][A-Za-z0-9_]*\\)$" $next) -}}
{{- fail "Cloud Connect --token must expand an environment variable (for example \"$(SPICE_ENROLL_KEY)\" from a Kubernetes Secret) — no chart value accepts a literal enrollment key. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $tokenEnvName = trimSuffix ")" (trimPrefix "$(" $next) -}}
{{- end -}}
{{- if hasPrefix "--token=" $arg -}}
{{- $tokenArgCount = add1 $tokenArgCount -}}
{{- $value := trimPrefix "--token=" $arg -}}
{{- if not (regexMatch "^\\$\\([A-Za-z_][A-Za-z0-9_]*\\)$" $value) -}}
{{- fail "Cloud Connect --token must expand an environment variable (for example \"$(SPICE_ENROLL_KEY)\" from a Kubernetes Secret) — no chart value accepts a literal enrollment key. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $tokenEnvName = trimSuffix ")" (trimPrefix "$(" $value) -}}
{{- end -}}
{{- end -}}
{{- if ne $tokenArgCount 1 -}}
{{- fail "Cloud Connect --token bootstrap requires exactly one --token argument. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $matchingEnvCount := 0 -}}
{{- $secretEnvCount := 0 -}}
{{- range .Values.additionalEnv -}}
{{- if eq (get . "name") $tokenEnvName -}}
{{- $matchingEnvCount = add1 $matchingEnvCount -}}
{{- $valueFrom := (get . "valueFrom") | default dict -}}
{{- $secretKeyRef := (get $valueFrom "secretKeyRef") | default dict -}}
{{- if and (not (hasKey . "value")) (get $secretKeyRef "name") (get $secretKeyRef "key") -}}
{{- $secretEnvCount = add1 $secretEnvCount -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if or (ne $matchingEnvCount 1) (ne $secretEnvCount 1) -}}
{{- fail (printf "Cloud Connect --token environment variable %s requires exactly one matching additionalEnv entry backed by valueFrom.secretKeyRef with a non-empty name and key; literal env values are forbidden. See deploy/chart/examples/cloud-connect/." $tokenEnvName) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Render a Kubernetes probe with built-in HTTP defaults.
*/}}
{{- define "spiceai.probe" -}}
{{- $probe := .probe -}}
{{- if $probe -}}
{{- if or $probe.exec $probe.tcpSocket $probe.grpc -}}
{{- omit $probe "httpGet" | toYaml -}}
{{- else -}}
{{- toYaml $probe -}}
{{- end -}}
{{- else -}}
httpGet:
  path: {{ .path }}
  port: {{ .port }}
timeoutSeconds: 1
periodSeconds: 10
failureThreshold: 3
{{- end -}}
{{- end -}}
