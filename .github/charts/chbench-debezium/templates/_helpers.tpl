{{- define "chbench-debezium.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{- define "chbench-debezium.selectorLabels" -}}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}
