#!/usr/bin/env python3
"""Shows that node C6's tests pin their guards.

Each mutation below breaks one guard (in `aspen serve`, the Helm chart, a
Terraform module or the release tooling), runs the check that should catch
it, and restores the file. Every line must say "FAILS (caught)". Run from the
repository root:

    python3 crates/aspen-cli/scripts/check-mutations.py [name-filter]

Needs cargo, git and curl; the chart mutations need helm and PyYAML, the
Terraform ones terraform with the modules initialised (deploy/README.md,
"Checks without a cloud account"). It edits files in place while it runs; do
not run it with uncommitted changes to those files. Exits non-zero if any
mutation is not caught.

There is no M5: dropping axum's `http2` feature from aspen-cli is not a
mutation of behaviour, because object_store's reqwest already enables
hyper-util's HTTP/2 support in the same build. The explicit feature records
the dependency; `dev_serves_push_and_clone_and_keeps_data_across_restarts`
pins h2c itself.
"""
import os
import re
import subprocess
import sys

CLI_UNIT = ["cargo", "test", "-q", "-p", "aspen-cli", "--bin", "aspen", "--"]
CLI_E2E = ["cargo", "test", "-q", "-p", "aspen-cli", "--test", "cli", "--"]
CHART = [sys.executable, "deploy/helm/check-chart.py"]


def tf(module):
    # The module reuses its example root's lock file (deploy/.gitignore).
    return ["sh", "-c", f"cd deploy/terraform/{module} && cp examples/*/.terraform.lock.hcl . "
            "&& TMPDIR=/tmp terraform init -backend=false -lockfile=readonly >/dev/null && TMPDIR=/tmp terraform test"]


muts = [
    ("M1 a password in DATABASE_URL is accepted", "crates/aspen-cli/src/config.rs",
     "            if parsed.password().is_some() {", "            if false && parsed.password().is_some() {",
     CLI_UNIT + ["a_password_in_the_database_url"]),
    ("M2 aspen dev listens beyond loopback", "crates/aspen-cli/src/main.rs",
     "    if !listen.ip().is_loopback() {", "    if false && !listen.ip().is_loopback() {",
     CLI_E2E + ["dev_refuses_to_listen_beyond_loopback"]),
    ("M3 the server's own express route is never mounted", "crates/aspen-cli/src/config.rs",
     "        .any(|u| u == own)", "        .any(|u| u == own && false)",
     CLI_E2E + ["serve_mounts_its_own_express_route"]),
    ("M4 /readyz ignores the database", "crates/aspen-cli/src/serve.rs",
     "        Err(_) => (\n            StatusCode::SERVICE_UNAVAILABLE,", "        Err(_) => (\n            StatusCode::OK,",
     CLI_UNIT + ["readyz_follows_the_metadata_database"]),
    ("M6 tokens not read from the metadata database", "crates/aspen-cli/src/serve.rs",
     "AuthService::new(config.policy, SqlTokens(meta.clone()))",
     "AuthService::new(config.policy, aspen_auth::MemoryTokenStore::new())",
     CLI_E2E + ["serve_enforces_tokens_end_to_end"]),
    ("M7 anonymous readers by default", "crates/aspen-cli/src/config.rs",
     '"ASPEN_POLICY_FILE")?.unwrap_or_default();',
     '"ASPEN_POLICY_FILE")?.unwrap_or_else(|| serde_json::from_str(r#"{"anonymous":["read:*"]}"#).unwrap());',
     CLI_E2E + ["serve_enforces_tokens_end_to_end"]),
    ("M8 SIGTERM is not a graceful shutdown", "crates/aspen-cli/src/serve.rs",
     "    .with_graceful_shutdown(until_stopped(stopped))\n", "",
     CLI_E2E + ["serve_enforces_tokens_end_to_end"]),
    ("M9 a local URL secret without a public URL", "crates/aspen-cli/src/config.rs",
     "                if public_url.is_none() {", "                if false && public_url.is_none() {",
     CLI_UNIT + ["the_local_url_secret_needs"]),
    ("M10 inline and file policy both accepted", "crates/aspen-cli/src/config.rs",
     '        (Some(_), Some(_)) => Err(invalid(inline, format!("set {inline} or {file}, not both"))),',
     "        (Some(_), Some(_)) => Ok(None),",
     CLI_UNIT + ["policy_and_limits_can_be_inline"]),
    ("M22 metrics on the public Git listener", "crates/aspen-cli/src/config.rs",
     "            metrics_on_git_listener: false,", "            metrics_on_git_listener: true,",
     CLI_E2E + ["serve_enforces_tokens_end_to_end"]),
    ("M23 SSH keys not read from the metadata database", "crates/aspen-cli/src/serve.rs",
     "        .with_ssh_keys(SqlSshKeys(meta.clone()));", "        .with_ssh_keys(aspen_auth::NoSshKeys);",
     CLI_E2E + ["serve_runs_git_over_ssh"]),
    ("M24 SSH without a host key accepted", "crates/aspen-cli/src/config.rs",
     "            (Some(_), None) => {\n                return Err(invalid(\n                    \"ASPEN_SSH_LISTEN\",",
     "            (Some(_), None) => None,\n            (Some(_), None) => {\n                return Err(invalid(\n                    \"ASPEN_SSH_LISTEN\",",
     CLI_UNIT + ["ssh_needs_a_stable_host_key"]),
    # The mirror service runs on a detached server, so sync passes still
    # import, but the server clients reach has no push-through hook.
    ("M27 the serving server never gets Server::with_mirror", "crates/aspen-cli/src/serve.rs",
     "    let (server, mirror) = server\n        .with_mirror(",
     "    let (_detached, mirror) = Server::new(meta.clone(), blobs.clone(), ServerConfig::new(max_object))\n"
     "        .with_receive_pack(ReceiveConfig::new(config.spool_dir.join(\"detached\"), max_object, max_inflated))?\n"
     "        .with_mirror(",
     CLI_E2E + ["serve_runs_mirror_sync_passes_and_proxy_push_through"]),
    ("M28 Mirror::run never started (no sync passes)", "crates/aspen-cli/src/serve.rs",
     "    let mirror_task = tokio::spawn(mirror.run(worker_id(label)?));",
     "    let mirror_task = tokio::spawn(async move { drop(mirror) });",
     CLI_E2E + ["serve_runs_mirror_sync_passes_and_proxy_push_through"]),
    ("M29 an incomplete mirror credential accepted", "crates/aspen-cli/src/config.rs",
     '                .map_err(|e| ConfigError::Mirror(format!("ASPEN_MIRROR_CREDENTIALS: {e}")))?;',
     "                .unwrap_or_default();",
     CLI_UNIT + ["mirror_credentials_are_checked"]),
    ("M25 chart SSH without a host key", "deploy/helm/aspen/templates/_helpers.tpl",
     "{{- if and .Values.ssh.enabled (not .Values.ssh.hostKeySecret.name) -}}", "{{- if false -}}", CHART),
    ("M26 chart exposes metrics through the Service", "deploy/helm/aspen/templates/service.yaml",
     "      targetPort: http\n", "      targetPort: http\n      protocol: TCP\n    - name: metrics\n      port: 9464\n      targetPort: metrics\n", CHART),
    ("M30 chart passes a mirror token as a plain value", "deploy/helm/aspen/templates/_helpers.tpl",
     '- name: ASPEN_MIRROR_{{ $infix }}_TOKEN\n  valueFrom:\n    secretKeyRef: {name: {{ .name | quote }}, key: {{ .key | default "token" | quote }}}',
     '- name: ASPEN_MIRROR_{{ $infix }}_TOKEN\n  value: {{ .name | quote }}', CHART),
    ("M31 chart accepts secret-looking plain values in extraEnv", "deploy/helm/aspen/templates/_helpers.tpl",
     '{{- if and (regexMatch $secretish (.name | default "")) (hasKey . "value") -}}', "{{- if false -}}", CHART),
    ("M32 chart trusts one proxy without an ingress", "deploy/helm/aspen/templates/_helpers.tpl",
     '{{- ternary "1" "0" (.Values.ingress.enabled | default false) -}}', '{{- "1" -}}', CHART),
    ("M11 chart accepts a password in database.url", "deploy/helm/aspen/templates/_helpers.tpl",
     '{{- if or (regexMatch "^postgres(ql)?://[^/@]*:[^/@]*@" $db) (regexMatch "[?&]password=" $db) -}}',
     '{{- if false -}}', CHART),
    ("M12 chart spool in memory", "deploy/helm/aspen/templates/deployment.yaml",
     "          emptyDir:\n            sizeLimit: {{ .Values.spool.sizeLimit }}",
     "          emptyDir:\n            medium: Memory\n            sizeLimit: {{ .Values.spool.sizeLimit }}", CHART),
    ("M13 chart liveness depends on the database", "deploy/helm/aspen/templates/deployment.yaml",
     "httpGet: {path: /healthz, port: http}", "httpGet: {path: /readyz, port: http}", CHART),
    ("M14 chart sets a variable aspen does not read", "deploy/helm/aspen/templates/deployment.yaml",
     "            - name: ASPEN_PREFETCH_MEMORY_BYTES", "            - name: ASPEN_PREFETCH_BYTES", CHART),
    ("M15 Cloud Run reachable from the internet", "deploy/terraform/gcp/run.tf",
     'ingress             = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"', 'ingress             = "INGRESS_TRAFFIC_ALL"',
     tf("gcp")),
    ("M33 Cloud Run checks invoker IAM (every request 403)", "deploy/terraform/gcp/run.tf",
     "  invoker_iam_disabled = true\n", "", tf("gcp")),
    ("M16 Cloud Run push limit beyond the spool", "deploy/terraform/gcp/run.tf",
     "condition     = var.max_push_bytes * 2 <= var.spool_size_mib * 1048576", "condition     = var.max_push_bytes > 0",
     tf("gcp")),
    ("M17 Cloud CDN serves stale packs", "deploy/terraform/gcp/lb.tf",
     "serve_while_stale = 0", "serve_while_stale = 86400", tf("gcp")),
    ("M18 Azure secret inline in the app", "deploy/terraform/azure/app.tf",
     '    name                = "db-password"\n    key_vault_secret_id = azurerm_key_vault_secret.db_password.versionless_id\n    identity            = azurerm_user_assigned_identity.aspen.id',
     '    name  = "db-password"\n    value = "inline"', tf("azure")),
    ("M19 Hetzner firewall opens PostgreSQL", "deploy/terraform/hetzner-minio/main.tf",
     '    port        = "443"\n    source_ips  = ["0.0.0.0/0", "::/0"]\n  }\n',
     '    port        = "443"\n    source_ips  = ["0.0.0.0/0", "::/0"]\n  }\n\n  rule {\n    direction  = "in"\n    protocol   = "tcp"\n    port       = "5432"\n    source_ips = ["0.0.0.0/0"]\n  }\n',
     tf("hetzner-minio")),
    ("M20 Hetzner accepts an unpinned image", "deploy/terraform/hetzner-minio/variables.tf",
     'condition     = can(regex("(:v?[0-9]+\\\\.[0-9]+\\\\.[0-9]+[^:/]*|@sha256:[0-9a-f]{64})$", var.aspen_image))',
     "condition     = length(var.aspen_image) > 0", tf("hetzner-minio")),
    ("M34 a publish request from a branch goes ahead", ".github/workflows/release.yml",
     '          if [ "$WANT_PUBLISH" = true ] && [ -z "$TAG" ]; then', "          if false; then",
     [sys.executable, "deploy/release/test_release.py"]),
    ("M35 the registry token interpolated into the script", ".github/workflows/release.yml",
     '          printf \'%s\' "$REGISTRY_TOKEN" |', '          printf \'%s\' "${{ secrets.GITHUB_TOKEN }}" |',
     [sys.executable, "deploy/release/test_release.py"]),
    ("M21 a release tag that does not match the version", "deploy/release/release.py",
     '    if tag is not None and tag != f"v{version}":', "    if False:",
     [sys.executable, "deploy/release/test_release.py"]),
]

only = sys.argv[1] if len(sys.argv) > 1 else ""
missed = False
for name, path, old, new, cmd in muts:
    if only and only not in name:
        continue
    src = open(path).read()
    edits = old if new is None else [(old, new)]
    mutated = src
    for o, n in edits:
        assert mutated.count(o) == 1, (name, o, mutated.count(o))
        mutated = mutated.replace(o, n)
    open(path, "w").write(mutated)
    try:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, env=dict(os.environ), timeout=900)
        except subprocess.TimeoutExpired:
            print(f"{name}: FAILS (caught: timed out)", flush=True)
            continue
        verdict = "FAILS (caught)" if r.returncode != 0 else "PASSES (NOT caught)"
        out = re.sub(r"\x1b\[[0-9;]*m", "", r.stdout + r.stderr).splitlines()
        why = [l for l in out if ("FAILED" in l and l.startswith("test ")) or l.startswith("FAIL:")
               or re.search(r'run ".*"\.\.\. fail', l) or "Missing expected failure" in l or "AssertionError" in l
               or l.startswith("KeyError")]
        print(f"{name}: {verdict} {why[:2]}", flush=True)
        missed = missed or r.returncode == 0
    finally:
        open(path, "w").write(src)
sys.exit(1 if missed else 0)
