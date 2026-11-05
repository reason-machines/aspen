#!/bin/sh
# Generates the stack's secrets once, on the server, into the data volume, so
# they survive a server rebuild and never pass through Terraform.
set -eu
env_file="$1"
if [ -e "$env_file" ]; then
  exit 0
fi
rand() { od -An -tx1 -N24 /dev/urandom | tr -d ' \n'; }
umask 077
cat >"$env_file" <<EOT
POSTGRES_PASSWORD=$(rand)
MINIO_ROOT_PASSWORD=$(rand)
ASPEN_S3_SECRET_KEY=$(rand)
EOT
