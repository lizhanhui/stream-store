{{/*
Common labels
*/}}
{{- define "stream-store.labels" -}}
app.kubernetes.io/name: stream-store
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Full image reference
*/}}
{{- define "stream-store.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}
