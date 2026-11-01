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
The validated `role` value: server (the default), sidecar, scheduler, or executor.
*/}}
{{- define "spiceai.role" -}}
{{- $role := .Values.role | default "server" -}}
{{- if not (has $role (list "server" "sidecar" "scheduler" "executor")) -}}
{{- fail (printf "role '%s' is not a Spice role. Use server (the default), sidecar, scheduler, or executor." $role) -}}
{{- end -}}
{{- $role -}}
{{- end -}}

{{/*
Whether the role joins a cluster: a scheduler or an executor.
*/}}
{{- define "spiceai.clusterNode" -}}
{{- if has (include "spiceai.role" .) (list "scheduler" "executor") -}}true{{- end -}}
{{- end -}}

{{/*
The `spiced` arguments that select the role, after the command. A server takes
none, so its pods start exactly as before the chart had a role.
*/}}
{{- define "spiceai.roleArgs" -}}
{{- $role := include "spiceai.role" . -}}
{{- if ne $role "server" }}
- --role
- {{ $role }}
{{- end }}
{{- if include "spiceai.clusterNode" . }}
{{- $cluster := .Values.cluster }}
{{- if eq $role "scheduler" }}
{{- $runtime := .Values.spicepod.runtime | default dict }}
{{- if not (or (dig "state" "location" "" $runtime) (dig "scheduler" "state_location" "" $runtime)) }}
{{- fail "role: scheduler requires `runtime.state.location` in `spicepod`, where schedulers keep shared cluster state. See: https://spiceai.org/docs/features/distributed-query" }}
{{- end }}
{{- end }}
- --node-bind-address
- {{ printf "0.0.0.0:%v" $cluster.port | quote }}
- --node-advertise-address
- {{ $cluster.advertiseAddress | default "$(SPICE_POD_IP)" | quote }}
{{- if eq $role "executor" }}
- --scheduler-address
- {{ required "role: executor requires `cluster.schedulerAddress`, the scheduler's cluster address, e.g. https://spice-scheduler-spiceai:50052" $cluster.schedulerAddress | quote }}
{{- end }}
{{- if $cluster.tls.secretName }}
- --node-mtls-ca-certificate-file
- /etc/spice/cluster-tls/ca.crt
- --node-mtls-certificate-file
- /etc/spice/cluster-tls/tls.crt
- --node-mtls-key-file
- /etc/spice/cluster-tls/tls.key
{{- else if $cluster.allowInsecureConnections }}
- --allow-insecure-connections
{{- else }}
{{- fail (printf "role: %s requires cluster mTLS: set `cluster.tls.secretName` to a Secret holding ca.crt, tls.crt and tls.key, or set `cluster.allowInsecureConnections: true` for development and testing." $role) }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
The readiness path a role serves. An executor serves only `/health`: it has no
`/v1/ready`, so probing that path would keep it from ever becoming ready.
*/}}
{{- define "spiceai.readinessPath" -}}
{{- if eq (include "spiceai.role" .) "executor" -}}/health{{- else -}}/v1/ready{{- end -}}
{{- end -}}

{{/*
`readinessProbe` with the default `/v1/ready` path moved to the role's own
readiness path. Any other path is the user's choice and stays as written.
*/}}
{{- define "spiceai.readinessProbe" -}}
{{- $probe := deepCopy (.Values.readinessProbe | default dict) -}}
{{- if and $probe.httpGet (eq ($probe.httpGet.path | default "") "/v1/ready") -}}
{{- $_ := set $probe.httpGet "path" (include "spiceai.readinessPath" .) -}}
{{- end -}}
{{- if $probe }}{{ toYaml $probe }}{{ end -}}
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
