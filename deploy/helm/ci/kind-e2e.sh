#!/usr/bin/env bash
# End-to-end test of the Helm chart on a local Kubernetes cluster (kind):
# installs the chart with two replicas and SSH against a throwaway
# PostgreSQL and MinIO, runs `helm test`, creates a repository and a token
# with the admin commands, pushes and clones with stock git over HTTP
# (`fsck --strict`), clones over SSH with a key registered by `aspen key
# add`, checks that /-/metrics answers on the metrics port and not through
# the Service, deletes a server pod and clones again through the other one.
#
#   deploy/helm/ci/kind-e2e.sh <image>      # e.g. aspen:ci, already in kind
#
# Needs kubectl pointing at the cluster, helm, git and bash.
set -euo pipefail

image="${1:?usage: kind-e2e.sh <repository:tag>}"
ns="aspen-e2e"
here="$(cd "$(dirname "$0")" && pwd)"
chart="$here/../aspen"
work="$(mktemp -d)"
pf_pid=""
pf_ssh_pid=""
pf_metrics_pid=""
ok=0

k() { kubectl --namespace "$ns" "$@"; }
cleanup() {
  for p in "$pf_pid" "$pf_ssh_pid" "$pf_metrics_pid"; do
    if [ -n "$p" ]; then kill "$p" 2>/dev/null || true; fi
  done
  if [ "$ok" != 1 ]; then
    echo "--- e2e failed; cluster state follows ---" >&2
    k get all -o wide >&2 || true
    k describe pods >&2 || true
    k logs -l app.kubernetes.io/name=aspen --all-containers --tail=200 >&2 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT
step() { printf '\n== %s\n' "$*"; }
rand() { od -An -tx1 -N24 /dev/urandom | tr -d ' \n'; }

step "backing services"
kubectl create namespace "$ns"
k create secret generic aspen-db --from-literal=password="$(rand)"
k create secret generic aspen-s3 --from-literal=AWS_ACCESS_KEY_ID=aspen-e2e \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(rand)"
k apply -f "$here/backing-services.yaml"
k rollout status deploy/postgres --timeout=180s
k rollout status deploy/minio --timeout=180s
k wait --for=condition=complete job/make-bucket --timeout=180s
ssh-keygen -q -t ed25519 -N '' -C aspen-e2e-host -f "$work/host_key"
k create secret generic aspen-ssh-host-key --from-file=host_key="$work/host_key"
ssh-keygen -q -t ed25519 -N '' -C aspen-e2e-user -f "$work/user_key"

step "helm install (2 replicas)"
helm install aspen "$chart" --namespace "$ns" --wait --timeout 5m \
  --set image.repository="${image%:*}" --set image.tag="${image##*:}" --set image.pullPolicy=Never \
  --set database.url='postgres://aspen@postgres:5432/aspen?sslmode=disable' \
  --set database.passwordSecret.name=aspen-db \
  --set blob.url=s3://aspen-packs --set blob.credentialsSecret=aspen-s3 \
  --set blob.env.AWS_ENDPOINT_URL=http://minio:9000 --set-string blob.env.AWS_ALLOW_HTTP=true \
  --set blob.env.AWS_REGION=us-east-1 --set packfileUris=false \
  --set resources.requests.cpu=100m --set resources.requests.memory=128Mi \
  --set spool.sizeLimit=2Gi --set terminationGracePeriodSeconds=30 \
  --set ssh.enabled=true --set ssh.hostKeySecret.name=aspen-ssh-host-key --set ssh.service.type=ClusterIP
k get pods -l app.kubernetes.io/name=aspen

step "helm test"
helm test aspen --namespace "$ns" --timeout 2m

step "repository, token, push and clone"
k exec deploy/aspen -- aspen repo create e2e/repo
token="$(k exec deploy/aspen -- aspen token create --subject e2e --grant write:e2e/repo)"
k port-forward svc/aspen 17080:80 >"$work/pf.log" 2>&1 &
pf_pid=$!
for _ in $(seq 1 30); do
  curl -fsS http://127.0.0.1:17080/readyz >/dev/null 2>&1 && break
  sleep 1
done
g() {
  GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 \
    GIT_AUTHOR_NAME=e2e GIT_AUTHOR_EMAIL=e2e@example.com \
    GIT_COMMITTER_NAME=e2e GIT_COMMITTER_EMAIL=e2e@example.com git "$@"
}
url="http://e2e:$token@127.0.0.1:17080/e2e/repo.git"
g init -q -b main "$work/src"
for i in 1 2 3; do
  echo "line $i" >"$work/src/f$i.txt"
  g -C "$work/src" add .
  g -C "$work/src" commit -q -m "commit $i"
done
tip="$(g -C "$work/src" rev-parse HEAD)"
if g -C "$work/src" push -q "http://127.0.0.1:17080/e2e/repo.git" main 2>/dev/null; then
  echo "an anonymous push was accepted" >&2
  exit 1
fi
g -C "$work/src" push -q "$url" main
clone() {
  rm -rf "$work/clone"
  g clone -q "$url" "$work/clone"
  [ "$(g -C "$work/clone" rev-parse HEAD)" = "$tip" ]
  g -C "$work/clone" fsck --strict
}
clone
echo "push, clone, fsck --strict: ok"

step "Git over SSH with a registered key"
k exec deploy/aspen -- aspen key add "$(cat "$work/user_key.pub")" --subject e2e-ssh --grant read:e2e/repo
k port-forward svc/aspen-ssh 17022:22 >"$work/pf-ssh.log" 2>&1 &
pf_ssh_pid=$!
sleep 3
GIT_SSH_COMMAND="ssh -i $work/user_key -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$work/known_hosts" \
  g clone -q "ssh://git@127.0.0.1:17022/e2e/repo.git" "$work/ssh-clone"
[ "$(g -C "$work/ssh-clone" rev-parse HEAD)" = "$tip" ]
echo "clone over SSH: ok"

step "metrics stay off the Service"
if curl -fsS http://127.0.0.1:17080/-/metrics >/dev/null 2>&1; then
  echo "/-/metrics answered through the Git Service" >&2
  exit 1
fi
k port-forward deploy/aspen 19464:9464 >"$work/pf-metrics.log" 2>&1 &
pf_metrics_pid=$!
sleep 3
# Save the page before grepping it: under pipefail, grep -q exits on its first
# match and curl then fails writing to the closed pipe (exit 23) once the page
# outgrows the pipe buffer.
curl -fsS http://127.0.0.1:19464/-/metrics >"$work/metrics.txt"
grep -q '^aspen_jobs' "$work/metrics.txt"
echo "metrics on the metrics port only: ok"

step "lose a pod, clone again"
k delete pod "$(k get pods -l app.kubernetes.io/component=server -o name | head -1 | cut -d/ -f2)" --wait=false
kill "$pf_pid" 2>/dev/null || true
k rollout status deploy/aspen --timeout=180s
k port-forward svc/aspen 17080:80 >"$work/pf.log" 2>&1 &
pf_pid=$!
for _ in $(seq 1 30); do
  curl -fsS http://127.0.0.1:17080/readyz >/dev/null 2>&1 && break
  sleep 1
done
clone
echo "clone after losing a pod: ok"

ok=1
step "helm e2e passed"
