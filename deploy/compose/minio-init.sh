#!/bin/sh
# Creates Aspen's pack bucket and a least-privilege MinIO user for Aspen.
# Run by the `minio-init` compose service; idempotent, so it is safe on every
# `docker compose up`.
#
# Inputs (environment): MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, ASPEN_BUCKET,
# ASPEN_S3_ACCESS_KEY, ASPEN_S3_SECRET_KEY, and optionally MINIO_URL.
set -eu

MINIO_URL="${MINIO_URL:-http://minio:9000}"
POLICY=aspen-packs-rw
POLICY_FILE="${TMPDIR:-/tmp}/aspen-policy.json"

mc alias set local "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc mb --ignore-existing "local/$ASPEN_BUCKET"

# Objects in the bucket only: no bucket creation, deletion or policy changes.
cat >"$POLICY_FILE" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"],
      "Resource": ["arn:aws:s3:::$ASPEN_BUCKET"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": ["arn:aws:s3:::$ASPEN_BUCKET/*"]
    }
  ]
}
EOF
mc admin policy create local "$POLICY" "$POLICY_FILE" >/dev/null

# `user add` creates the user or resets its secret, so rotating
# ASPEN_S3_SECRET_KEY in .env takes effect on the next run.
mc admin user add local "$ASPEN_S3_ACCESS_KEY" "$ASPEN_S3_SECRET_KEY" >/dev/null
if ! mc admin user info local "$ASPEN_S3_ACCESS_KEY" --json | grep -q "\"$POLICY\""; then
  mc admin policy attach local "$POLICY" --user "$ASPEN_S3_ACCESS_KEY" >/dev/null
fi

echo "minio-init: bucket '$ASPEN_BUCKET' ready; user '$ASPEN_S3_ACCESS_KEY' has policy '$POLICY'"
