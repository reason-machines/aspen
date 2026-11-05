#!/bin/sh
# Starts a throwaway MinIO in Docker for aspen-blob's S3 conformance tests
# and prints the ASPEN_TEST_S3_* settings they read. Usage:
#
#   eval "$(crates/aspen-blob/scripts/test-minio.sh)"
#   cargo test -p aspen-blob
#
# In GitHub Actions the settings go to $GITHUB_ENV instead. The credentials
# are fixed test values for a container that listens on 127.0.0.1 only.
# Stop it with: docker rm -f aspen-test-minio
#
# The image is the one deploy/compose/compose.yaml pins, by the same digest;
# bump both together.
set -eu

NAME=aspen-test-minio
IMAGE="${MINIO_IMAGE:-cgr.dev/chainguard/minio:latest-dev@sha256:604ce09e2099ab20704a4c3bf25863bbb9bdaa643e7f522dc652098a6284bd6a}"
PORT="${ASPEN_TEST_MINIO_PORT:-9000}"
USER=aspen-test
PASSWORD=aspen-test-secret
BUCKET=aspen-test

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:9000" \
  -e MINIO_ROOT_USER="$USER" -e MINIO_ROOT_PASSWORD="$PASSWORD" \
  "$IMAGE" server /data --address :9000 >/dev/null

i=0
until curl -fs "http://127.0.0.1:$PORT/minio/health/live" >/dev/null; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    docker logs "$NAME" >&2
    echo "MinIO did not become healthy" >&2
    exit 1
  fi
  sleep 1
done
docker exec -e MC_CONFIG_DIR=/tmp/mc "$NAME" \
  mc alias set local http://127.0.0.1:9000 "$USER" "$PASSWORD" >/dev/null
docker exec -e MC_CONFIG_DIR=/tmp/mc "$NAME" mc mb --ignore-existing "local/$BUCKET" >/dev/null

settings="ASPEN_TEST_S3_ENDPOINT=http://127.0.0.1:$PORT
ASPEN_TEST_S3_BUCKET=$BUCKET
ASPEN_TEST_S3_ACCESS_KEY_ID=$USER
ASPEN_TEST_S3_SECRET_ACCESS_KEY=$PASSWORD"
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "$settings" >>"$GITHUB_ENV"
else
  echo "$settings" | sed 's/^/export /'
fi
