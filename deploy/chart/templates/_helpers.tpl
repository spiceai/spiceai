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
Whether the container command carries a Cloud Connect `--token` bootstrap
argument, in either its two-element (`--token`, `$(VAR)`) or single-element
(`--token=$(VAR)`) form.
*/}}
{{- define "spiceai.cloudConnectTokenBootstrap" -}}
{{- $found := false -}}
{{- range .Values.command -}}
{{- if or (eq . "--token") (hasPrefix "--token=" .) -}}
{{- $found = true -}}
{{- end -}}
{{- end -}}
{{- if $found }}true{{ end -}}
{{- end -}}

{{/*
Validate a Cloud Connect `--token` bootstrap deployment before rendering.

One enrollment key enrolls exactly one identity, and the identity must
survive a pod replacement, so a command carrying `--token` requires:
  - exactly one replica (an absent replicaCount renders a 1-replica
    workload and is accepted),
  - stateful persistent storage,
  - SPICE_CONFIG_DIR set to a literal path beneath the stateful mountPath,
  - the token argument to be an env expansion `$(VAR)` (normally from a
    Secret) — never a literal key baked into a chart value.
*/}}
{{- define "spiceai.validateCloudConnectBootstrap" -}}
{{- if include "spiceai.cloudConnectTokenBootstrap" . -}}
{{- if or (ne (int (.Values.replicaCount | default 1)) 1) (not .Values.stateful.enabled) -}}
{{- fail "Cloud Connect --token bootstrap requires one replica and persistent Spice identity storage. Set replicaCount: 1 and stateful.enabled: true, and set SPICE_CONFIG_DIR beneath stateful.mountPath. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $mount := clean (.Values.stateful.mountPath | default "") -}}
{{- $configDirOk := false -}}
{{- range .Values.additionalEnv -}}
{{- if and (eq (get . "name") "SPICE_CONFIG_DIR") (get . "value") -}}
{{- $configDir := clean (get . "value") -}}
{{- if and (ne $mount ".") (hasPrefix (printf "%s/" $mount) $configDir) -}}
{{- $configDirOk = true -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not $configDirOk -}}
{{- fail "Cloud Connect --token bootstrap requires one replica and persistent Spice identity storage. Add an additionalEnv entry setting SPICE_CONFIG_DIR to a path beneath stateful.mountPath (for example /data/.spice under mountPath /data). See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- $cmd := .Values.command -}}
{{- range $i, $arg := $cmd -}}
{{- if eq $arg "--token" -}}
{{- $next := "" -}}
{{- if lt (add1 $i) (len $cmd) -}}
{{- $next = index $cmd (add1 $i) -}}
{{- end -}}
{{- if not (regexMatch "^\\$\\([A-Za-z_][A-Za-z0-9_]*\\)$" $next) -}}
{{- fail "Cloud Connect --token must expand an environment variable (for example \"$(SPICE_ENROLL_KEY)\" from a Kubernetes Secret) — no chart value accepts a literal enrollment key. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- end -}}
{{- if hasPrefix "--token=" $arg -}}
{{- $value := trimPrefix "--token=" $arg -}}
{{- if not (regexMatch "^\\$\\([A-Za-z_][A-Za-z0-9_]*\\)$" $value) -}}
{{- fail "Cloud Connect --token must expand an environment variable (for example \"$(SPICE_ENROLL_KEY)\" from a Kubernetes Secret) — no chart value accepts a literal enrollment key. See deploy/chart/examples/cloud-connect/." -}}
{{- end -}}
{{- end -}}
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
