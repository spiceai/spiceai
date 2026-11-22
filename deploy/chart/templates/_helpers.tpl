{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "spiceai.name" -}}
{{- regexReplaceAll "-+$" (default .Chart.Name .Values.nameOverride | trunc 63) "" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
A name must end in a letter or digit, so every trailing `-` the cut leaves is removed, not only the last.
If release name contains chart name it will be used as a full name.
*/}}
{{- define "spiceai.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- regexReplaceAll "-+$" (.Values.fullnameOverride | trunc 63) "" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- regexReplaceAll "-+$" (.Release.Name | trunc 63) "" -}}
{{- else -}}
{{- regexReplaceAll "-+$" (printf "%s-%s" .Release.Name $name | trunc 63) "" -}}
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
{{- regexReplaceAll "-+$" (printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63) "" -}}
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
Cluster nodes (schedulers and executors) run as a StatefulSet behind a headless
Service, so each pod has a stable DNS name its certificate can cover with one
wildcard SAN (see https://spiceai.org/docs/features/distributed-query). A
StatefulSet is also used when `stateful.enabled` requests one for any role.
*/}}
{{- define "spiceai.useStatefulSet" -}}
{{- if or .Values.stateful.enabled (include "spiceai.clusterNode" .) -}}true{{- end -}}
{{- end -}}

{{/*
The name of the Deployment or StatefulSet. A Deployment takes `<fullname>`. A StatefulSet name
is at most 52 characters, because Kubernetes builds two values from it that must stay within 63
characters: each pod's name, `<name>-<ordinal>`, which is also the first DNS label of the
address a cluster node advertises; and each pod's `controller-revision-hash` label value,
`<name>-<hash>`, whose hash is up to 10 characters.

A StatefulSet takes `<fullname>` when it fits. A longer fullname is cut to 45 characters, with
every trailing `-` the cut leaves removed, and followed by `-` and the first 6 hex characters of
the SHA-256 of the whole fullname. Two releases whose fullnames differ only after the cut
therefore get different names, and a release keeps its name across upgrades.
*/}}
{{- define "spiceai.workloadName" -}}
{{- $fullname := include "spiceai.fullname" . -}}
{{- if and (include "spiceai.useStatefulSet" .) (gt (len $fullname) 52) -}}
{{- printf "%s-%s" (regexReplaceAll "-+$" ($fullname | trunc 45) "") (sha256sum $fullname | trunc 6) -}}
{{- else -}}
{{- $fullname -}}
{{- end -}}
{{- end -}}

{{/*
The headless Service that gives cluster node pods their stable per-pod DNS names, and so the
name a wildcard certificate for those pods covers. A Service name is a DNS label of at most 63
characters. A fullname of at most 54 characters gives `<fullname>-headless`. A longer fullname is
cut to 47 characters, with every trailing `-` the cut leaves removed, and followed by `-`, the
first 6 hex characters of the SHA-256 of the whole fullname, and `-headless`. Two releases whose
fullnames differ only after the cut therefore get different Services, and a release keeps its
name across upgrades.

The name must also differ from the main Service's, `<fullname>`. Only a 62- or 63-character
fullname that ends in its own hash and `-headless` could match; it is cut to 45 characters
instead, which gives a shorter name.
*/}}
{{- define "spiceai.headlessServiceName" -}}
{{- $fullname := include "spiceai.fullname" . -}}
{{- if le (len $fullname) 54 -}}
{{- printf "%s-headless" $fullname -}}
{{- else -}}
{{- $hash := sha256sum $fullname | trunc 6 -}}
{{- $name := printf "%s-%s-headless" (regexReplaceAll "-+$" ($fullname | trunc 47) "") $hash -}}
{{- if eq $name $fullname -}}
{{- $name = printf "%s-%s-headless" (regexReplaceAll "-+$" ($fullname | trunc 45) "") $hash -}}
{{- end -}}
{{- $name -}}
{{- end -}}
{{- end -}}

{{/*
The address a cluster node advertises to its peers: its stable per-pod DNS name,
`$(POD_NAME).<headless-service>.<namespace>.svc.cluster.local`, which a wildcard
certificate `*.<headless-service>.<namespace>.svc.cluster.local` covers.
`cluster.advertiseAddress` overrides it.
*/}}
{{- define "spiceai.advertiseAddress" -}}
{{- if .Values.cluster.advertiseAddress -}}
{{- .Values.cluster.advertiseAddress -}}
{{- else -}}
{{- printf "$(POD_NAME).%s.%s.svc.cluster.local" (include "spiceai.headlessServiceName" .) (include "spiceai.namespace" .) -}}
{{- end -}}
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
- {{ include "spiceai.advertiseAddress" . | quote }}
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

{{/*
The chart's authentication posture:
  spicepod  the Spicepod configures `runtime.auth` itself, and the chart adds nothing;
  api_key   the chart supplies an API key from a Secret (the default);
  none      no API key.
*/}}
{{- define "spiceai.authMode" -}}
{{- $runtime := .Values.spicepod.runtime | default dict -}}
{{- if hasKey $runtime "auth" -}}
spicepod
{{- else -}}
{{- $mode := (.Values.auth | default dict).mode | default "api_key" -}}
{{- if not (has $mode (list "api_key" "none")) -}}
{{- fail (printf "auth.mode is '%s'. Set it to api_key (the default) or none." $mode) -}}
{{- end -}}
{{- $mode -}}
{{- end -}}
{{- end -}}

{{/*
The render failure for a NodePort or LoadBalancer Service in front of a runtime that does not
authenticate its callers; empty when the runtime authenticates every caller. It decides as
`spiced` does, from the rendered Spicepod and the flags in `command` that override it:

  - client certificates: TLS is on (`runtime.tls.enabled: true`, or `--tls-enabled true`)
    and requires one (`runtime.tls.client_auth_mode: required`, or `--tls-client-auth-mode
    required`, which takes precedence). This authenticates callers whatever `runtime.auth`
    says. `request` admits a caller without a certificate, and `client_auth_mode` does
    nothing while TLS is off, so neither counts;
  - an API key: `runtime.auth` has an `api-key` entry (or its alias `api_key`) whose
    `enabled` is not false. `auth.mode: api_key` writes one and `auth.mode: none` writes
    none; a Spicepod that sets `runtime.auth` is read as written.

`--set-runtime auth…` or `--set-runtime tls…` in `command` changes these after the chart has
read them, so the chart refuses unless the path it does not change authenticates.

`command` is read one argument at a time, as `spiced`'s parser reads it: a flag's value is the
argument after it, or the text after its `=`, and text inside a value is never read as a flag.
An argument that starts with `-` is a flag, so it is never the value of the flag before it.
Everything after `--` is a positional argument.
*/}}
{{- define "spiceai.unauthenticatedExposure" -}}
{{- $type := .Values.service.type -}}
{{- $runtime := .Values.spicepod.runtime | default dict -}}
{{- $flags := list "--tls-enabled" "--tls-client-auth-mode" "--set-runtime" -}}
{{- $values := dict "--tls-enabled" (list) "--tls-client-auth-mode" (list) "--set-runtime" (list) -}}
{{- $pending := "" -}}
{{- $positional := false -}}
{{- range $arg := .Values.command | default list -}}
{{- $arg = toString $arg -}}
{{- if $positional -}}
{{- else if and $pending (not (hasPrefix "-" $arg)) -}}
{{- $_ := set $values $pending (append (get $values $pending) $arg) -}}
{{- $pending = "" -}}
{{- else -}}
{{- $pending = "" -}}
{{- if eq $arg "--" -}}
{{- $positional = true -}}
{{- else if has $arg $flags -}}
{{- $pending = $arg -}}
{{- else -}}
{{- range $flag := $flags -}}
{{- if hasPrefix (printf "%s=" $flag) $arg -}}
{{- $_ := set $values $flag (append (get $values $flag) (trimPrefix (printf "%s=" $flag) $arg)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $authOverridden := false -}}
{{- $tlsOverridden := false -}}
{{- range $value := get $values "--set-runtime" -}}
{{- if regexMatch "^auth[.=]" $value -}}
{{- $authOverridden = true -}}
{{- end -}}
{{- if regexMatch "^tls(=|\\.enabled=|\\.client_auth_mode=)" $value -}}
{{- $tlsOverridden = true -}}
{{- end -}}
{{- end -}}
{{- $tls := dict -}}
{{- if kindIs "map" $runtime.tls -}}
{{- $tls = $runtime.tls -}}
{{- end -}}
{{- $tlsFlagValues := get $values "--tls-enabled" -}}
{{- $tlsFlagOn := gt (len $tlsFlagValues) 0 -}}
{{- range $value := $tlsFlagValues -}}
{{- if ne $value "true" -}}
{{- $tlsFlagOn = false -}}
{{- end -}}
{{- end -}}
{{- $tlsOn := or (and (kindIs "bool" $tls.enabled) $tls.enabled) $tlsFlagOn -}}
{{- $certRequired := eq (toString $tls.client_auth_mode) "required" -}}
{{- $modeFlagValues := get $values "--tls-client-auth-mode" -}}
{{- if $modeFlagValues -}}
{{- $certRequired = true -}}
{{- range $value := $modeFlagValues -}}
{{- if ne $value "required" -}}
{{- $certRequired = false -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not (and $tlsOn $certRequired (not $tlsOverridden)) -}}
{{- $reason := "" -}}
{{- $fix := "" -}}
{{- if not $authOverridden -}}
{{- $mode := include "spiceai.authMode" . -}}
{{- $removeAuth := "remove spicepod.runtime.auth and the chart sets an API key (auth.mode: api_key), or use a ClusterIP Service." -}}
{{- if eq $mode "none" -}}
{{- $reason = "auth.mode is none" -}}
{{- $fix = "Set auth.mode to api_key, or use a ClusterIP Service." -}}
{{- else if eq $mode "spicepod" -}}
{{- $auth := $runtime.auth -}}
{{- $addKey := printf "Add an `api-key` entry with `keys` to spicepod.runtime.auth, or %s" $removeAuth -}}
{{- if kindIs "invalid" $auth -}}
{{- $reason = "spicepod.runtime.auth is null" -}}
{{- $fix = $addKey -}}
{{- else if not (kindIs "map" $auth) -}}
{{- $reason = "spicepod.runtime.auth is not a mapping" -}}
{{- $fix = $addKey -}}
{{- else if not (or (hasKey $auth "api-key") (hasKey $auth "api_key")) -}}
{{- $reason = ternary "spicepod.runtime.auth is empty" "spicepod.runtime.auth has no `api-key` entry" (empty $auth) -}}
{{- $fix = $addKey -}}
{{- else -}}
{{- range $name := list "api-key" "api_key" -}}
{{- if and (not $reason) (hasKey $auth $name) -}}
{{- $apiKey := index $auth $name -}}
{{- $setting := printf "spicepod.runtime.auth.%s" $name -}}
{{- if not (kindIs "map" $apiKey) -}}
{{- $reason = printf "%s is %s" $setting (ternary "null" "not a mapping" (kindIs "invalid" $apiKey)) -}}
{{- $fix = printf "Set %s.keys, or %s" $setting $removeAuth -}}
{{- else if and (kindIs "bool" $apiKey.enabled) (not $apiKey.enabled) -}}
{{- $reason = printf "%s.enabled is false" $setting -}}
{{- $fix = printf "Set %s.enabled to true, or use a ClusterIP Service." $setting -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if or $authOverridden (and $reason $tlsOverridden) -}}
{{- printf "command passes --set-runtime for runtime.auth or runtime.tls and service.type is %s: the chart cannot tell whether the runtime authenticates callers outside the cluster. Set runtime.auth and runtime.tls in spicepod.runtime instead, or use a ClusterIP Service." $type -}}
{{- else if $reason -}}
{{- printf "%s and service.type is %s: the runtime would serve callers outside the cluster without an API key. %s" $reason $type $fix -}}
{{- if and $certRequired (not $tlsOn) }} spicepod.runtime.tls.client_auth_mode: required takes effect only with spicepod.runtime.tls.enabled: true.{{ end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The Secret holding the API key under `api-key`: `auth.apiKey.existingSecret`, or the one the
chart creates.
*/}}
{{- define "spiceai.apiKeySecretName" -}}
{{- $apiKey := (.Values.auth | default dict).apiKey | default dict -}}
{{- $apiKey.existingSecret | default (printf "%s-spiceai-api-key" .Release.Name | trunc 63 | trimSuffix "-") -}}
{{- end -}}

{{/*
Whether this render can read the cluster: `helm template`, a client-side dry run and Argo CD
cannot, and `lookup` then answers nothing. Listing Secrets in the release namespace answers a
list, empty or not, whenever the cluster is readable, including before the namespace exists.
*/}}
{{- define "spiceai.clusterReadable" -}}
{{- if lookup "v1" "Secret" (include "spiceai.namespace" .) "" -}}true{{- end -}}
{{- end -}}

{{/*
The Spicepod the runtime reads: `spicepod`, plus what `auth.mode` adds to it.

api_key: `runtime.auth` with one read-write key, the `SPICE_API_KEY` that the Deployment sets
from the API key Secret. A `${ store:KEY }` reference resolves only through a store the
Spicepod declares, and the runtime adds its own `env` store only when the Spicepod declares
none, so the key is read through:
  - `env`, when `secrets` is empty;
  - else the first `from: env` store without `params`, which reads the process environment
    first (a `file_path` store reads its file first, and the file could hold another key);
  - else a `from: env` store the chart adds, with a name no declared store has. It goes first
    in `secrets`: the last-declared store wins `${ secrets:KEY }`, so the chart's store is
    consulted after every store the Spicepod declares.

none: the Service's own names, so that in-cluster clients that address the Service by name are
answered. They join the hosts list the Spicepod already has, `runtime.allowed_hosts` or the
deprecated `runtime.mcp.allowed_hosts` (the runtime refuses to start with both), or
`runtime.allowed_hosts` when it has neither.
*/}}
{{- define "spiceai.spicepod" -}}
{{- $pod := deepCopy .Values.spicepod -}}
{{- $runtime := $pod.runtime | default dict -}}
{{- $mode := include "spiceai.authMode" . -}}
{{- if eq $mode "api_key" -}}
{{- $store := "env" -}}
{{- $secrets := $pod.secrets | default list -}}
{{- if $secrets -}}
{{- $names := list -}}
{{- $envStores := list -}}
{{- range $secrets -}}
{{- $names = append $names .name -}}
{{- if and (eq (toString .from) "env") (not .params) -}}
{{- $envStores = append $envStores .name -}}
{{- end -}}
{{- end -}}
{{- if $envStores -}}
{{- $store = first $envStores -}}
{{- else -}}
{{- $store = "spiceai_chart_env" -}}
{{- range $i := until 10 -}}
{{- if has $store $names -}}
{{- $store = printf "spiceai_chart_env_%d" (add1 $i) -}}
{{- end -}}
{{- end -}}
{{- if has $store $names -}}
{{- fail "spicepod.secrets declares stores named spiceai_chart_env through spiceai_chart_env_10, so the chart has no name left for the store it reads the API key through. Add a `from: env` store without `params` to spicepod.secrets, and the chart reads the key through that store." -}}
{{- end -}}
{{- $_ := set $pod "secrets" (prepend $secrets (dict "from" "env" "name" $store "description" "Added by the Helm chart to read the API key in SPICE_API_KEY.")) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $runtime "auth" (dict "api-key" (dict "enabled" true "keys" (list (printf "${ %s:SPICE_API_KEY }:rw" $store)))) -}}
{{- else if eq $mode "none" -}}
{{- $name := include "spiceai.fullname" . -}}
{{- $namespace := include "spiceai.namespace" . -}}
{{- $serviceHosts := list $name (printf "%s.%s" $name $namespace) (printf "%s.%s.svc" $name $namespace) (printf "%s.%s.svc.cluster.local" $name $namespace) -}}
{{- $mcp := $runtime.mcp | default dict -}}
{{- if and (kindIs "invalid" $runtime.allowed_hosts) (not (kindIs "invalid" $mcp.allowed_hosts)) -}}
{{- $_ := set $mcp "allowed_hosts" (concat $mcp.allowed_hosts $serviceHosts | uniq) -}}
{{- $_ := set $runtime "mcp" $mcp -}}
{{- else -}}
{{- $_ := set $runtime "allowed_hosts" (concat ($runtime.allowed_hosts | default list) $serviceHosts | uniq) -}}
{{- end -}}
{{- end -}}
{{- if $runtime -}}
{{- $_ := set $pod "runtime" $runtime -}}
{{- end -}}
{{- toYaml $pod -}}
{{- end -}}
