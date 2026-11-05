#!/usr/bin/env bash
# Brings the compose stack up in an isolated project, checks it, tears it down.
#
#   deploy/compose/smoke.sh
#
# Checks:
#   1. every long-running service reaches "healthy";
#   2. Aspen answers GET /healthz on the published port;
#   3. Postgres accepts the exact DATABASE_URL Aspen receives;
#   4. with the S3 credentials Aspen receives, the pack bucket exists and
#      objects can be written, read and deleted, but buckets cannot be created;
#   5. bucket setup (minio-init) is idempotent;
#   6. Aspen serves Git end to end: a repository and tokens made with the
#      admin commands, an anonymous push refused, a token push stored as a
#      pack in the bucket, a clone that passes `git fsck --strict`, and the
#      same clone again after Aspen restarts (state lives in Postgres and
#      MinIO, not the container); SIGTERM stops Aspen with exit code 0.
#
# Needs Docker with the compose plugin, git, curl and bash. Builds the Aspen
# image from this checkout, unless ASPEN_IMAGE names an image to test. Uses
# its own project name, port and throwaway credentials, so it does not touch
# a stack you already run from this directory.
set -euo pipefail

cd "$(dirname "$0")"

project="aspen-smoke-$$"
port="${SMOKE_HTTP_PORT:-17070}"
workdir="$(mktemp -d)"
env_file="$workdir/.env"

./init-env.sh "$env_file" >/dev/null
echo "ASPEN_HTTP_PORT=$port" >>"$env_file"

dc() { docker compose --project-name "$project" --env-file "$env_file" "$@"; }

ok=0
cleanup() {
  if [ "$ok" != 1 ]; then
    echo "--- smoke failed; service state and logs follow ---" >&2
    dc ps -a >&2 || true
    dc logs --no-color >&2 || true
  fi
  dc down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$workdir"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }

step "docker compose up"
if [ -n "${ASPEN_IMAGE:-}" ]; then
  echo "using image $ASPEN_IMAGE"
  dc up --detach --quiet-pull
else
  dc up --detach --quiet-pull --build
fi

step "waiting for services to be healthy"
deadline=$((SECONDS + 120))
for svc in postgres minio aspen; do
  while :; do
    status="$(docker inspect --format '{{.State.Health.Status}}' "$(dc ps -q "$svc")")"
    [ "$status" = healthy ] && break
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "$svc is '$status' after 120s" >&2
      exit 1
    fi
    sleep 1
  done
  echo "$svc: healthy"
done
init_exit="$(docker inspect --format '{{.State.ExitCode}}' "$(dc ps -aq minio-init)")"
echo "minio-init: exited $init_exit"
[ "$init_exit" = 0 ]

step "GET /healthz"
body="$(curl -fsS "http://127.0.0.1:$port/healthz")"
echo "http://127.0.0.1:$port/healthz -> $body"
[ "$body" = ok ]

step "Postgres accepts Aspen's DATABASE_URL"
# Over TCP with the URL and password Aspen gets, so the password is checked
# (only Unix-socket connections are trusted inside the Postgres container).
database_url="$(dc exec -T aspen printenv DATABASE_URL)"
pgpassword="$(dc exec -T aspen printenv PGPASSWORD)"
psql_as_aspen() { dc exec -T -e PGPASSWORD="$pgpassword" postgres psql "$database_url" -v ON_ERROR_STOP=1 -tA "$@"; }
answer="$(psql_as_aspen -c 'select 1')"
echo "select 1 -> $answer"
[ "$answer" = 1 ]
echo "server version: $(psql_as_aspen -c 'show server_version')"
if dc exec -T -e PGPASSWORD=wrong postgres psql "$database_url" -tAc 'select 1' >/dev/null 2>&1; then
  echo "Postgres accepted a wrong password over TCP" >&2
  exit 1
fi
echo "wrong password rejected: ok"

step "MinIO bucket is usable with Aspen's S3 credentials"
aspen_env() { dc exec -T aspen printenv "$1"; }
AWS_ACCESS_KEY_ID="$(aspen_env AWS_ACCESS_KEY_ID)"
AWS_SECRET_ACCESS_KEY="$(aspen_env AWS_SECRET_ACCESS_KEY)"
AWS_ENDPOINT_URL="$(aspen_env AWS_ENDPOINT_URL)"
blob_url="$(aspen_env ASPEN_BLOB_URL)"
BUCKET="${blob_url#s3://}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL BUCKET
# The script runs inside the container, so its variables expand there.
# shellcheck disable=SC2016
dc run --rm --no-deps -T \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_ENDPOINT_URL -e BUCKET \
  --entrypoint /bin/sh minio-init -c '
    set -eu
    mc alias set aspen "$AWS_ENDPOINT_URL" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null
    mc stat --quiet "aspen/$BUCKET" >/dev/null && echo "bucket $BUCKET exists"
    echo pack-bytes | mc pipe --quiet "aspen/$BUCKET/smoke/object" >/dev/null
    [ "$(mc cat "aspen/$BUCKET/smoke/object")" = pack-bytes ] && echo "object write/read: ok"
    mc rm --quiet "aspen/$BUCKET/smoke/object" >/dev/null && echo "object delete: ok"
    if mc mb --quiet aspen/not-allowed >/dev/null 2>&1; then
      echo "Aspen credentials can create buckets; expected least privilege" >&2
      exit 1
    fi
    echo "bucket creation denied: ok"
  '

step "minio-init is idempotent"
dc run --rm -T minio-init

step "Aspen serves Git"
wait_healthy() {
  local deadline=$((SECONDS + 60))
  until [ "$(docker inspect --format '{{.State.Health.Status}}' "$(dc ps -q aspen)")" = healthy ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "aspen is not healthy after 60s" >&2
      exit 1
    fi
    sleep 1
  done
}
dc exec -T aspen aspen repo create smoke/repo
token="$(dc exec -T aspen aspen token create --subject smoke --grant write:smoke/repo)"
url="http://127.0.0.1:$port/smoke/repo.git"
authed="http://smoke:$token@127.0.0.1:$port/smoke/repo.git"
g() {
  GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 \
    GIT_AUTHOR_NAME=smoke GIT_AUTHOR_EMAIL=smoke@example.com \
    GIT_COMMITTER_NAME=smoke GIT_COMMITTER_EMAIL=smoke@example.com git "$@"
}
work="$workdir/work"
g init -q -b main "$work"
for i in 1 2 3; do
  echo "line $i" >"$work/file$i.txt"
  g -C "$work" add .
  g -C "$work" commit -q -m "commit $i"
done
tip="$(g -C "$work" rev-parse HEAD)"
if g -C "$work" push -q "$url" main 2>/dev/null; then
  echo "an anonymous push was accepted" >&2
  exit 1
fi
echo "anonymous push refused: ok"
g -C "$work" push -q "$authed" main
echo "token push: ok"
# shellcheck disable=SC2016  # expands inside the container
packs="$(dc run --rm --no-deps -T -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_ENDPOINT_URL -e BUCKET \
  --entrypoint /bin/sh minio-init -c '
    mc alias set aspen "$AWS_ENDPOINT_URL" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null
    mc ls --recursive "aspen/$BUCKET/repos/"' | grep -c '\.pack$' || true)"
echo "packs in the bucket: $packs"
[ "$packs" -ge 1 ]
clone_check() {
  rm -rf "$workdir/clone"
  g clone -q "$authed" "$workdir/clone"
  [ "$(g -C "$workdir/clone" rev-parse HEAD)" = "$tip" ]
  g -C "$workdir/clone" fsck --strict
}
clone_check
echo "clone matches and passes fsck --strict"
dc restart aspen >/dev/null
wait_healthy
clone_check
echo "clone after restart: ok"

step "SIGTERM stops Aspen cleanly"
dc stop aspen >/dev/null
code="$(docker inspect --format '{{.State.ExitCode}}' "$(dc ps -aq aspen)")"
echo "aspen exited $code"
[ "$code" = 0 ]

ok=1
step "smoke passed"
