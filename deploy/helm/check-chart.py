#!/usr/bin/env python3
"""Checks the Aspen Helm chart without a cluster.

Renders deploy/helm/aspen with `helm template` under several value sets and
asserts what the chart promises (see deploy/README.md, "Kubernetes"):

- the server runs `aspen serve` as uid 10001 with a read-only root file
  system, no capabilities and no service-account token;
- secrets reach the pods only through secretKeyRef, never as plain values,
  mirror credentials included;
- the push spool is node disk or a PersistentVolumeClaim, never memory;
- liveness probes /healthz (not the database), readiness /readyz;
- /-/metrics is on its own port, which no Service or Ingress exposes, and
  an optional NetworkPolicy admits only the configured scrapers to it;
- read and push request rates are limited by default (issue #125);
- Git over SSH needs a host key from a Secret, mounted for the pod only, and
  its Service keeps client addresses (`externalTrafficPolicy: Local`);
- every ASPEN_* variable the chart sets is one the binary reads;
- unsafe or incomplete values fail the install with a message naming them.

Usage, from the repository root (needs helm and PyYAML; kubeconform, when on
PATH, also validates every rendering against the Kubernetes schemas):

    python3 deploy/helm/check-chart.py

Exits non-zero on the first failed check.
"""
import json
import re
import shutil
import subprocess
import sys

import yaml

CHART = "deploy/helm/aspen"
MINIMAL = f"{CHART}/ci/minimal-values.yaml"
KUBE_VERSION = "1.30.0"


def render(*sets, values=MINIMAL, expect_ok=True):
    args = ["helm", "template", "t", CHART, "-f", values, "--namespace", "git"]
    for s in sets:
        if s.startswith("string:"):
            args += ["--set-string", s[len("string:"):]]
        else:
            args += ["--set-json" if s.startswith("policy=") else "--set", s]
    r = subprocess.run(args, capture_output=True, text=True)
    if not expect_ok:
        return r
    if r.returncode != 0:
        sys.exit(f"helm template {sets} failed:\n{r.stderr}")
    if shutil.which("kubeconform"):
        k = subprocess.run(
            ["kubeconform", "-strict", "-summary", "-kubernetes-version", KUBE_VERSION, "-"],
            input=r.stdout, capture_output=True, text=True,
        )
        if k.returncode != 0:
            sys.exit(f"kubeconform rejected the rendering of {sets}:\n{k.stdout}{k.stderr}")
    return [d for d in yaml.safe_load_all(r.stdout) if d]


def find(docs, kind, name=None):
    for d in docs:
        if d["kind"] == kind and (name is None or d["metadata"]["name"] == name):
            return d
    return None


def env_of(container):
    return {e["name"]: e for e in container.get("env", [])}


passed = 0


def check(cond, what):
    global passed
    if not cond:
        sys.exit(f"FAIL: {what}")
    passed += 1
    print(f"ok: {what}")


def check_secret_modes(docs, what, expect_any):
    """No file of a Secret volume is readable by other users, which aspen
    refuses at startup (issue #125). Kubernetes' default is 0644."""
    modes = []
    for d in docs:
        if d["kind"] not in ("Deployment", "StatefulSet", "Pod"):
            continue
        spec = d["spec"]["template"]["spec"] if "template" in d["spec"] else d["spec"]
        for v in spec.get("volumes", []):
            if "secret" not in v:
                continue
            modes.append((v["name"], v["secret"].get("defaultMode", 0o644)))
            modes += [(f'{v["name"]}/{i["path"]}', i["mode"]) for i in v["secret"].get("items", []) if "mode" in i]
    check((modes or not expect_any) and all(m & 0o007 == 0 for _, m in modes),
          f"{what}: no Secret file is readable by other users (issue #125; {modes})")


def binary_variables():
    """ASPEN_* names the binary reads (CLI and express settings)."""
    names = set()
    for path in ["crates/aspen-cli/src/config.rs", "crates/aspen-cli/src/main.rs",
                 "crates/aspen-express/src/settings.rs", "crates/aspen-mirror/src/settings.rs"]:
        names |= set(re.findall(r'"(ASPEN_[A-Z0-9_]+)"', open(path).read()))
    return names


# Per-credential mirror variables, ASPEN_MIRROR_<NAME>_<SUFFIX>: the suffixes
# aspen_mirror::settings::credentials reads.
MIRROR_SUFFIXES = set(re.findall(r'(?:var|from_file)\("([A-Z_]+)"\)',
                                 open("crates/aspen-mirror/src/settings.rs").read()))


def read_by_aspen(name, known, credentials=()):
    if name in known:
        return True
    return any(name.startswith(f"ASPEN_MIRROR_{c.upper().replace('-', '_')}_")
               and name[len(f"ASPEN_MIRROR_{c}_"):] in MIRROR_SUFFIXES for c in credentials)


SECRETISH = r"(?i)(secret|token|password)|_key$"


# --- the minimal rendering -------------------------------------------------
docs = render()
dep = find(docs, "Deployment", "t-aspen")
pod = dep["spec"]["template"]["spec"]
c = pod["containers"][0]
env = env_of(c)

check(c["args"] == ["serve"], "the server runs `aspen serve`")
check(pod["securityContext"]["runAsNonRoot"] and pod["securityContext"]["runAsUser"] == 10001,
      "pods run as the image's unprivileged uid 10001")
sc = c["securityContext"]
check(sc["readOnlyRootFilesystem"] and not sc["allowPrivilegeEscalation"]
      and sc["capabilities"]["drop"] == ["ALL"], "read-only root, no privilege escalation, no capabilities")
sa = find(docs, "ServiceAccount")
check(sa["automountServiceAccountToken"] is False, "no Kubernetes API token in the pods")

check(env["PGPASSWORD"]["valueFrom"]["secretKeyRef"] == {"name": "aspen-db", "key": "password"},
      "PGPASSWORD comes from the named Secret")
check(env["DATABASE_URL"]["value"].startswith("postgres://aspen@"), "DATABASE_URL carries no password")
plain = {k: v.get("value", "") for k, v in env.items() if "value" in v}
check(not any(re.search(SECRETISH, k) for k in plain),
      f"no secret-named variable has a plain value ({sorted(plain)})")
check("ASPEN_EXPRESS_KEYS" not in env, "no express keys unless express is configured")

vols = {v["name"]: v for v in pod["volumes"]}
check("emptyDir" in vols["spool"] and "medium" not in vols["spool"]["emptyDir"],
      "the spool is node disk (emptyDir without medium: Memory)")
mounts = {m["mountPath"]: m["name"] for m in c["volumeMounts"]}
check(mounts["/var/lib/aspen/spool"] == "spool" and env["ASPEN_SPOOL_DIR"]["value"] == "/var/lib/aspen/spool",
      "ASPEN_SPOOL_DIR points at the spool volume")
check(c["livenessProbe"]["httpGet"]["path"] == "/healthz", "liveness probes /healthz (not the database)")
check(c["readinessProbe"]["httpGet"]["path"] == "/readyz", "readiness probes /readyz")
check(env["ASPEN_TRUSTED_PROXY_HOPS"]["value"] == "0", "no trusted proxy without the chart's ingress: the TCP peer")

svc = find(docs, "Service", "t-aspen")
labels = dep["spec"]["template"]["metadata"]["labels"]
check(all(labels.get(k) == v for k, v in svc["spec"]["selector"].items()), "the Service selects the server pods")
check([p["targetPort"] for p in svc["spec"]["ports"]] == ["http"], "the Service exposes Git over HTTP only")
ports = {p["name"]: p["containerPort"] for p in c["ports"]}
check(ports.get("metrics") == 9464 and env["ASPEN_METRICS_LISTEN"]["value"] == "0.0.0.0:9464"
      and ports["http"] != 9464, "metrics listen on their own container port (C5)")
annotations = dep["spec"]["template"]["metadata"]["annotations"]
check(annotations.get("prometheus.io/port") == "9464" and annotations.get("prometheus.io/path") == "/-/metrics",
      "pods advertise the metrics port for scraping")
check("ASPEN_SSH_LISTEN" not in env and "ssh" not in ports and find(docs, "Service", "t-aspen-ssh") is None,
      "no SSH unless enabled")
cm = find(docs, "ConfigMap", "t-aspen")
check(json.loads(cm["data"]["policy.json"]) == {}, "an empty policy (tokens only) by default")
limits = json.loads(cm["data"]["limits.json"])
check(limits.get("read", {}).get("per_second", 0) > 0 and limits.get("push", {}).get("per_second", 0) > 0,
      f"read and push request rates are limited by default (issue #125; limits.json {limits})")
check(find(docs, "NetworkPolicy") is None, "no NetworkPolicy unless enabled")
check(find(docs, "PodDisruptionBudget") is not None, "a disruption budget with two replicas")
check(find(docs, "Ingress") is None and find(docs, "Deployment", "t-aspen-express") is None,
      "no ingress or express nodes unless enabled")
check(c["image"] == "ghcr.io/reason-machines/aspen:v0.0.0", "the image tag defaults to v<appVersion>")

known = binary_variables()
chart_vars = {n for n in env if n.startswith("ASPEN_")}
check(chart_vars <= known, f"every ASPEN_* variable the chart sets is read by aspen (unknown: {sorted(chart_vars - known)})")

test_pod = find(docs, "Pod")
check(test_pod["spec"]["containers"][0]["args"][:1] == ["healthcheck"]
      and test_pod["spec"]["containers"][0]["args"][-1].endswith("/readyz"), "`helm test` checks /readyz")

# --- options ---------------------------------------------------------------
before = dep["spec"]["template"]["metadata"]["annotations"]["checksum/config"]
docs = render('policy={"anonymous":["read:public/*"]}')
dep2 = find(docs, "Deployment", "t-aspen")
cm = find(docs, "ConfigMap", "t-aspen")
check(json.loads(cm["data"]["policy.json"]) == {"anonymous": ["read:public/*"]}, "policy values reach policy.json")
check(dep2["spec"]["template"]["metadata"]["annotations"]["checksum/config"] != before,
      "a policy change rolls the pods")

docs = render("spool.ephemeralVolume.enabled=true", "spool.ephemeralVolume.storageClassName=gp3")
vols = {v["name"]: v for v in find(docs, "Deployment", "t-aspen")["spec"]["template"]["spec"]["volumes"]}
check(vols["spool"]["ephemeral"]["volumeClaimTemplate"]["spec"]["storageClassName"] == "gp3",
      "the spool can be a per-pod PersistentVolumeClaim")

docs = render("image.digest=sha256:" + "a" * 64)
check(find(docs, "Deployment", "t-aspen")["spec"]["template"]["spec"]["containers"][0]["image"]
      == "ghcr.io/reason-machines/aspen@sha256:" + "a" * 64, "a digest pins the image")

docs = render("express.keysSecret.name=aspen-express", "express.standalone.enabled=true",
              "express.urls={https://express.git.example.com}", "express.standalone.ingress.enabled=true",
              "express.standalone.ingress.host=express.git.example.com", "ingress.enabled=true",
              "ingress.host=git.example.com", "publicUrl=https://git.example.com")
server = env_of(find(docs, "Deployment", "t-aspen")["spec"]["template"]["spec"]["containers"][0])
express = find(docs, "Deployment", "t-aspen-express")["spec"]["template"]["spec"]["containers"][0]
xenv = env_of(express)
key_ref = {"name": "aspen-express", "key": "keys"}
check(server["ASPEN_EXPRESS_KEYS"]["valueFrom"]["secretKeyRef"] == key_ref
      and xenv["ASPEN_EXPRESS_KEYS"]["valueFrom"]["secretKeyRef"] == key_ref,
      "server and express nodes share the signing keys from one Secret")
check(server["ASPEN_EXPRESS_URLS"]["value"] == "https://express.git.example.com", "express URLs reach the server")
check(express["args"] == ["express"] and xenv["ASPEN_BLOB_URL"]["value"] == "s3://aspen-packs",
      "express nodes run `aspen express` on the same bucket")
check(express["readinessProbe"]["httpGet"]["path"] == "/-/health", "express nodes are probed at /-/health")
hosts = sorted(i["spec"]["rules"][0]["host"] for i in docs if i["kind"] == "Ingress")
check(hosts == ["express.git.example.com", "git.example.com"], "Git and express each get their own host")
check({n for n in xenv if n.startswith("ASPEN_")} <= known, "every ASPEN_* variable of the express nodes is read by aspen")

docs = render("ssh.enabled=true", "ssh.hostKeySecret.name=aspen-ssh-host-key", "ingress.enabled=true",
              "ingress.host=git.example.com")
pod = find(docs, "Deployment", "t-aspen")["spec"]["template"]["spec"]
c = pod["containers"][0]
env = env_of(c)
vols = {v["name"]: v for v in pod["volumes"]}
mounts = {m["name"]: m["mountPath"] for m in c["volumeMounts"]}
check(env["ASPEN_SSH_LISTEN"]["value"] == "0.0.0.0:2222"
      and env["ASPEN_SSH_HOST_KEY_FILE"]["value"] == mounts["ssh-host-key"] + "/host_key",
      "SSH listens with the host key file from its Secret (C3)")
check(vols["ssh-host-key"]["secret"]["secretName"] == "aspen-ssh-host-key"
      and vols["ssh-host-key"]["secret"]["defaultMode"] == 0o440, "the host key Secret is readable by the pod group only")
psc = pod["securityContext"]
check(psc["fsGroup"] == psc["runAsGroup"] == 10001,
      "Secret volumes belong to aspen's own group (fsGroup = runAsGroup), so the secret-file check accepts them")
check_secret_modes(docs, "the SSH install", expect_any=True)
ssh_svc = find(docs, "Service", "t-aspen-ssh")
check(ssh_svc["spec"]["type"] == "LoadBalancer" and ssh_svc["spec"]["ports"][0]["port"] == 22
      and ssh_svc["spec"]["ports"][0]["targetPort"] == "ssh", "SSH gets its own Service on port 22")
# SSH limits key on the TCP peer (docs/SSH.md); with the default `Cluster`
# policy kube-proxy rewrites it to a node's address (issue #120).
for svc_type in ["LoadBalancer", "NodePort"]:
    svc = find(render("ssh.enabled=true", "ssh.hostKeySecret.name=k", f"ssh.service.type={svc_type}"),
               "Service", "t-aspen-ssh")
    check(svc["spec"]["type"] == svc_type and svc["spec"].get("externalTrafficPolicy") == "Local",
          f"a {svc_type} SSH Service keeps client addresses (externalTrafficPolicy: Local)")
svc = find(render("ssh.enabled=true", "ssh.hostKeySecret.name=k", "ssh.service.type=ClusterIP"),
           "Service", "t-aspen-ssh")
check("externalTrafficPolicy" not in svc["spec"], "a ClusterIP SSH Service has no external traffic policy")
check(env["ASPEN_TRUSTED_PROXY_HOPS"]["value"] == "1", "one trusted proxy (the ingress controller) with the ingress")
ingress = find(docs, "Ingress", "t-aspen")
backends = [p["backend"]["service"] for r in ingress["spec"]["rules"] for p in r["http"]["paths"]]
check(all(b["name"] == "t-aspen" and b["port"] == {"name": "http"} for b in backends),
      "the ingress reaches only the HTTP port, never metrics or SSH")

env = env_of(find(render("trustedProxyHops=2"), "Deployment", "t-aspen")["spec"]["template"]["spec"]["containers"][0])
check(env["ASPEN_TRUSTED_PROXY_HOPS"]["value"] == "2", "an explicit trustedProxyHops wins")
env = env_of(find(render("trustedProxyHops=0", "ingress.enabled=true", "ingress.host=g.example.com"),
                  "Deployment", "t-aspen")["spec"]["template"]["spec"]["containers"][0])
check(env["ASPEN_TRUSTED_PROXY_HOPS"]["value"] == "0", "trustedProxyHops=0 is kept, not taken as unset")

limits = json.loads(find(render("limits.read=null"), "ConfigMap", "t-aspen")["data"]["limits.json"])
check("read" not in limits and "push" in limits, "one default rate can be removed (limits.read=null)")

# --- metrics NetworkPolicy (issue #125) ---------------------------------------
scraper = {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}
docs = render("metrics.networkPolicy.enabled=true", "ssh.enabled=true", "ssh.hostKeySecret.name=k",
              "metrics.networkPolicy.from[0].namespaceSelector.matchLabels.kubernetes\\.io/metadata\\.name=monitoring")
np = find(docs, "NetworkPolicy", "t-aspen")
dep = find(docs, "Deployment", "t-aspen")
labels = dep["spec"]["template"]["metadata"]["labels"]
check(np is not None and np["spec"]["policyTypes"] == ["Ingress"]
      and all(labels.get(k) == v for k, v in np["spec"]["podSelector"]["matchLabels"].items())
      and np["spec"]["podSelector"]["matchLabels"].get("app.kubernetes.io/component") == "server",
      "the NetworkPolicy selects the server pods and governs their ingress")
rules = np["spec"]["ingress"]
# The policy names the container's ports, so it follows their numbers.
named = {p["name"]: p["containerPort"] for p in dep["spec"]["template"]["spec"]["containers"][0]["ports"]}
open_ports = sorted(p["port"] for r in rules if "from" not in r for p in r["ports"])
check(open_ports == ["http", "ssh"] and named["http"] == 7070 and named["ssh"] == 2222,
      f"Git over HTTP and SSH stay open to every source ({open_ports})")
metrics_rules = [r for r in rules if any(p["port"] == "metrics" for p in r["ports"])]
check(len(metrics_rules) == 1 and metrics_rules[0]["from"] == [scraper]
      and [p["port"] for p in metrics_rules[0]["ports"]] == ["metrics"] and named["metrics"] == 9464,
      "the metrics port admits only the configured scrapers")
check(all(p["port"] in named for r in rules for p in r["ports"]),
      "every port the NetworkPolicy names is a port of the server container")
docs = render("metrics.networkPolicy.enabled=true", "metrics.port=9100",
              "metrics.networkPolicy.from[0].podSelector.matchLabels.app=prometheus")
rules = find(docs, "NetworkPolicy", "t-aspen")["spec"]["ingress"]
named = {p["name"]: p["containerPort"]
         for p in find(docs, "Deployment", "t-aspen")["spec"]["template"]["spec"]["containers"][0]["ports"]}
check(sorted(p["port"] for r in rules if "from" not in r for p in r["ports"]) == ["http"]
      and [p["port"] for r in rules if "from" in r for p in r["ports"]] == ["metrics"] and named["metrics"] == 9100,
      "without SSH only HTTP is open, and the policy follows metrics.port")

# --- mirror mode (C4 obligation) ---------------------------------------------
docs = render("mirror.credentials[0].name=github", "mirror.credentials[0].tokenSecret.name=aspen-mirror-gh",
              "mirror.credentials[1].name=acme-app", "mirror.credentials[1].githubApp.appId=Iv1.abc",
              "string:mirror.credentials[1].githubApp.installationId=7890123",
              "mirror.credentials[1].githubApp.privateKeySecret.name=aspen-mirror-app", "mirror.syncSeconds=30")
env = env_of(find(docs, "Deployment", "t-aspen")["spec"]["template"]["spec"]["containers"][0])
check(env["ASPEN_MIRROR_CREDENTIALS"]["value"] == "github,acme-app" and env["ASPEN_MIRROR_SYNC_SECS"]["value"] == "30",
      "mirror credential names and the sync interval reach the server")
check(env["ASPEN_MIRROR_GITHUB_TOKEN"]["valueFrom"]["secretKeyRef"] == {"name": "aspen-mirror-gh", "key": "token"},
      "a mirror token comes from its Secret")
check(env["ASPEN_MIRROR_ACME_APP_GITHUB_PRIVATE_KEY"]["valueFrom"]["secretKeyRef"]
      == {"name": "aspen-mirror-app", "key": "private-key"}
      and env["ASPEN_MIRROR_ACME_APP_GITHUB_APP_ID"]["value"] == "Iv1.abc"
      and env["ASPEN_MIRROR_ACME_APP_GITHUB_INSTALLATION_ID"]["value"] == "7890123",
      "a GitHub App key comes from its Secret; its IDs are plain")
plain = {k for k, v in env.items() if "value" in v}
check(not any(re.search(SECRETISH, k) for k in plain), f"no mirror secret has a plain value ({sorted(plain)})")
unknown = sorted(n for n in env if n.startswith("ASPEN_") and not read_by_aspen(n, known, ["github", "acme-app"]))
check(not unknown, f"every mirror variable the chart sets is read by aspen (unknown: {unknown})")
# The chart passes mirror credentials as variables, not files, so the
# secret-file check does not apply to them; any Secret volume still must
# pass it, with SSH as well.
check(not any(n.startswith("ASPEN_MIRROR_") and n.endswith("_FILE") for n in env),
      "mirror credentials come from Secrets as variables, not as files")
check_secret_modes(docs, "the mirror install", expect_any=False)
mirror_sets = ["mirror.credentials[0].name=github", "mirror.credentials[0].tokenSecret.name=aspen-mirror-gh"]
check_secret_modes(render(*mirror_sets, "ssh.enabled=true", "ssh.hostKeySecret.name=k"),
                   "the mirror install with SSH", expect_any=True)

# --- refusals --------------------------------------------------------------
refusals = [
    (["database.url="], "database.url is required"),
    (["database.url=sqlite:///var/lib/aspen/meta.db"], "SQLite is not supported"),
    (["database.url=postgres://aspen:hunter2@db/aspen"], "must not contain a password"),
    (["database.url=postgres://aspen@db/aspen?sslmode=require&password=hunter2"], "must not contain a password"),
    (["database.passwordSecret.name="], "database.passwordSecret.name is required"),
    (["blob.url=file:///data"], "blob.url is required and must be s3://"),
    (["express.urls={https://express.example.com}"], "express.keysSecret.name is required"),
    (["express.cdnUrl=https://cdn.example.com"], "express.keysSecret.name is required"),
    (["express.standalone.enabled=true"], "express.keysSecret.name is required"),
    (["packfileUris=maybe"], "packfileUris must be auto, true or false"),
    (["express.cdnPastEnd=200"], "express.cdnPastEnd must be 416 or 206"),
    (["ingress.enabled=true"], "ingress.host is required"),
    (["spool.ephemeralVolume.enabled=true"], "storageClassName is required"),
    (["ssh.enabled=true"], "ssh.hostKeySecret.name is required"),
    (["metrics.networkPolicy.enabled=true"], "metrics.networkPolicy.from is required"),
    (["blob.env.AWS_SECRET_ACCESS_KEY=hunter2"], "blob.env.AWS_SECRET_ACCESS_KEY looks like a secret"),
    (["blob.env.AWS_SESSION_TOKEN=hunter2"], "blob.env.AWS_SESSION_TOKEN looks like a secret"),
    (["extraEnv[0].name=ASPEN_MIRROR_GITHUB_TOKEN", "extraEnv[0].value=hunter2"], "extraEnv ASPEN_MIRROR_GITHUB_TOKEN looks like a secret"),
    (["extraEnv[0].name=OTEL_EXPORTER_OTLP_PASSWORD", "extraEnv[0].value=hunter2"], "looks like a secret"),
    (["extraEnv[0].name=SIGNING_KEY", "extraEnv[0].value=hunter2"], "extraEnv SIGNING_KEY looks like a secret"),
    (["trustedProxyHops=many"], "trustedProxyHops must be a whole number"),
    (["mirror.credentials[0].name=GitHub", "mirror.credentials[0].tokenSecret.name=s"], "must be 1 to 64 of a-z"),
    (["mirror.credentials[0].name=gh"], "set exactly one of tokenSecret.name or githubApp"),
    (["mirror.credentials[0].name=gh", "mirror.credentials[0].tokenSecret.name=s",
      "mirror.credentials[0].githubApp.appId=1"], "set exactly one of tokenSecret.name or githubApp"),
    (["mirror.credentials[0].name=gh", "string:mirror.credentials[0].githubApp.appId=1",
      "string:mirror.credentials[0].githubApp.installationId=2"], "privateKeySecret.name is required"),
    (["mirror.credentials[0].name=gh", "mirror.credentials[0].githubApp.appId=1",
      "mirror.credentials[0].githubApp.installationId=2",
      "mirror.credentials[0].githubApp.privateKeySecret.name=k"], "as quoted strings"),
]
for sets, message in refusals:
    r = render(*sets, expect_ok=False)
    check(r.returncode != 0 and message in r.stderr, f"refused: {sets} ({message})")
    check("hunter2" not in r.stderr, f"the refusal of {sets} does not echo the secret")

print(f"\n{passed} chart checks passed")
