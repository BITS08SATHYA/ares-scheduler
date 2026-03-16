{{/*
Common labels
*/}}
{{- define "ares-local.labels" -}}
app.kubernetes.io/name: ares-scheduler
app.kubernetes.io/component: local
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
