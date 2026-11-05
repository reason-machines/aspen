{{/* Names and labels. */}}
{{- define "aspen.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "aspen.fullname" -}}
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

{{- define "aspen.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "aspen.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "aspen.selectorLabels" -}}
app.kubernetes.io/name: {{ include "aspen.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "aspen.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "aspen.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "aspen.image" -}}
{{- $repo := .Values.image.repository -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" $repo .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" $repo (default (printf "v%s" .Chart.AppVersion) .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Whether any express setting is in use, so the signing keys are needed. */}}
{{- define "aspen.expressUsed" -}}
{{- $e := .Values.express -}}
{{- if or $e.urls $e.cdnUrl (eq (toString $e.serve) "true") $e.standalone.enabled -}}true{{- end -}}
{{- end -}}

{{/*
Refuse values that would deploy a broken or unsafe server. Each message
names the value to fix.
*/}}
{{- define "aspen.validate" -}}
{{- $db := .Values.database.url | default "" -}}
{{- if not $db -}}
{{- fail "database.url is required: postgres://<user>@<host>:5432/<db>?sslmode=require" -}}
{{- end -}}
{{- if not (regexMatch "^postgres(ql)?://" $db) -}}
{{- fail "database.url must be a postgres:// URL: every replica shares one database, so SQLite is not supported here" -}}
{{- end -}}
{{- if or (regexMatch "^postgres(ql)?://[^/@]*:[^/@]*@" $db) (regexMatch "[?&]password=" $db) -}}
{{- fail "database.url must not contain a password; put it in the Secret named by database.passwordSecret" -}}
{{- end -}}
{{- if not .Values.database.passwordSecret.name -}}
{{- fail "database.passwordSecret.name is required: a Secret holding the database password (set as PGPASSWORD)" -}}
{{- end -}}
{{- $blob := .Values.blob.url | default "" -}}
{{- if not (hasPrefix "s3://" $blob) -}}
{{- fail "blob.url is required and must be s3://<bucket>[/<prefix>]: replicas cannot share a file:// store" -}}
{{- end -}}
{{- if not (has (toString .Values.packfileUris) (list "auto" "true" "false")) -}}
{{- fail "packfileUris must be auto, true or false" -}}
{{- end -}}
{{- if not (has (toString .Values.express.serve) (list "auto" "true" "false")) -}}
{{- fail "express.serve must be auto, true or false" -}}
{{- end -}}
{{- if not (has (toString .Values.express.cdnPastEnd) (list "416" "206")) -}}
{{- fail "express.cdnPastEnd must be 416 or 206" -}}
{{- end -}}
{{- if and (include "aspen.expressUsed" .) (not .Values.express.keysSecret.name) -}}
{{- fail "express.keysSecret.name is required when express URLs, a CDN or express nodes are configured" -}}
{{- end -}}
{{- if and .Values.ssh.enabled (not .Values.ssh.hostKeySecret.name) -}}
{{- fail "ssh.hostKeySecret.name is required when ssh.enabled: clients pin the host key, so every replica needs the same one" -}}
{{- end -}}
{{- $secretish := "(?i)(secret|token|password)|_key$" -}}
{{- range $k, $v := .Values.blob.env -}}
{{- if regexMatch $secretish $k -}}
{{- fail (printf "blob.env.%s looks like a secret: put it in the Secret named by blob.credentialsSecret, or in extraEnv with valueFrom.secretKeyRef" $k) -}}
{{- end -}}
{{- end -}}
{{- range .Values.extraEnv -}}
{{- if and (regexMatch $secretish (.name | default "")) (hasKey . "value") -}}
{{- fail (printf "extraEnv %s looks like a secret: use valueFrom.secretKeyRef, not a plain value" .name) -}}
{{- end -}}
{{- end -}}
{{- if not (or (kindIs "invalid" .Values.trustedProxyHops) (regexMatch "^[0-9]+$" (toString .Values.trustedProxyHops))) -}}
{{- fail "trustedProxyHops must be a whole number (or unset)" -}}
{{- end -}}
{{- range .Values.mirror.credentials -}}
{{- $n := .name | default "" -}}
{{- if not (regexMatch "^[a-z0-9_-]{1,64}$" $n) -}}
{{- fail (printf "mirror.credentials: name %q must be 1 to 64 of a-z, 0-9, - and _" $n) -}}
{{- end -}}
{{- $token := and .tokenSecret .tokenSecret.name -}}
{{- if eq (not $token) (not .githubApp) -}}
{{- fail (printf "mirror.credentials %s: set exactly one of tokenSecret.name or githubApp" $n) -}}
{{- end -}}
{{- with .githubApp -}}
{{- if not (and (kindIs "string" .appId) (kindIs "string" .installationId)) -}}
{{- fail (printf "mirror.credentials %s: githubApp.appId and githubApp.installationId are required, as quoted strings" $n) -}}
{{- end -}}
{{- if not (and .privateKeySecret .privateKeySecret.name) -}}
{{- fail (printf "mirror.credentials %s: githubApp.privateKeySecret.name is required" $n) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and .Values.metrics.networkPolicy.enabled (not .Values.metrics.networkPolicy.from) -}}
{{- fail "metrics.networkPolicy.from is required when metrics.networkPolicy.enabled: the peers (your Prometheus) that may scrape the metrics port; without any, a NetworkPolicy rule would admit every source" -}}
{{- end -}}
{{- if and .Values.spool.ephemeralVolume.enabled (not .Values.spool.ephemeralVolume.storageClassName) -}}
{{- fail "spool.ephemeralVolume.storageClassName is required when spool.ephemeralVolume.enabled" -}}
{{- end -}}
{{- end -}}

{{/* Settings shared by `aspen serve` and `aspen express`: the blob store and the express keys. */}}
{{- define "aspen.blobEnv" -}}
- name: ASPEN_BLOB_URL
  value: {{ .Values.blob.url | quote }}
{{- range $k, $v := .Values.blob.env }}
- name: {{ $k }}
  value: {{ $v | toString | quote }}
{{- end }}
{{- with .Values.blob.credentialsSecret }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef: {name: {{ . | quote }}, key: AWS_ACCESS_KEY_ID}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef: {name: {{ . | quote }}, key: AWS_SECRET_ACCESS_KEY}
{{- end }}
{{- if include "aspen.expressUsed" . }}
- name: ASPEN_EXPRESS_KEYS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.express.keysSecret.name | quote }}
      key: {{ .Values.express.keysSecret.key | quote }}
{{- end }}
{{- end -}}

{{/* ASPEN_TRUSTED_PROXY_HOPS: as set, else 1 behind the chart's ingress and 0 otherwise. */}}
{{- define "aspen.trustedProxyHops" -}}
{{- if kindIs "invalid" .Values.trustedProxyHops -}}
{{- ternary "1" "0" (.Values.ingress.enabled | default false) -}}
{{- else -}}
{{- .Values.trustedProxyHops | toString -}}
{{- end -}}
{{- end -}}

{{/* Mirror mode: credential names, and their secrets through secretKeyRef only. */}}
{{- define "aspen.mirrorEnv" -}}
{{- $names := list -}}
{{- range .Values.mirror.credentials -}}
{{- $names = append $names .name -}}
{{- end -}}
- name: ASPEN_MIRROR_SYNC_SECS
  value: {{ .Values.mirror.syncSeconds | int64 | quote }}
{{- if $names }}
- name: ASPEN_MIRROR_CREDENTIALS
  value: {{ join "," $names | quote }}
{{- end }}
{{- range .Values.mirror.credentials }}
{{- $infix := .name | upper | replace "-" "_" }}
{{- with .tokenSecret }}
- name: ASPEN_MIRROR_{{ $infix }}_TOKEN
  valueFrom:
    secretKeyRef: {name: {{ .name | quote }}, key: {{ .key | default "token" | quote }}}
{{- end }}
{{- with .username }}
- name: ASPEN_MIRROR_{{ $infix }}_USERNAME
  value: {{ . | quote }}
{{- end }}
{{- with .githubApp }}
- name: ASPEN_MIRROR_{{ $infix }}_GITHUB_APP_ID
  value: {{ .appId | quote }}
- name: ASPEN_MIRROR_{{ $infix }}_GITHUB_INSTALLATION_ID
  value: {{ .installationId | quote }}
- name: ASPEN_MIRROR_{{ $infix }}_GITHUB_PRIVATE_KEY
  valueFrom:
    secretKeyRef: {name: {{ .privateKeySecret.name | quote }}, key: {{ .privateKeySecret.key | default "private-key" | quote }}}
{{- with .apiUrl }}
- name: ASPEN_MIRROR_{{ $infix }}_GITHUB_API_URL
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}
