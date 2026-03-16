{{/*
Common labels
*/}}
{{- define "ares-global.labels" -}}
app.kubernetes.io/name: ares-scheduler
app.kubernetes.io/component: global
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
