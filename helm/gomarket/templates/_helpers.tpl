{{/*
Expand the name of the chart.
*/}}
{{- define "gomarket.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full name: release-chart, truncated to 63 chars.
*/}}
{{- define "gomarket.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "gomarket.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "gomarket.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (used by Deployment and Service matchLabels).
*/}}
{{- define "gomarket.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gomarket.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
