#!/usr/bin/env sh
# Fail when a generated secret of the AWS deployment could land in Terraform
# state or plan files.
#
# The AWS module and the public demo generate every secret (database
# passwords, express keys, the SSH host key) with ephemeral resources and
# write it with write-only attributes (`*_wo`), which Terraform never
# stores. Two mistakes would quietly undo that, and `terraform test` cannot
# see either, because mocked providers have no state worth reading:
#
# - a `resource "random_password"`, `"random_string"` or `"tls_private_key"`
#   instead of an `ephemeral` one: its result is stored in state;
# - a plain `secret_string`, `secret_binary`, `value` or `password` on a
#   Secrets Manager secret version, an SSM parameter or the RDS instance
#   instead of the `_wo` attribute.
#
# Usage (from the repository root):
#
#   deploy/scripts/check-terraform-write-only.sh [directory]
#
# The directory defaults to deploy/terraform/aws; the argument exists so
# the check itself can be tested against a deliberately broken copy.
set -eu

dir=${1:-deploy/terraform/aws}
status=0

stored=$(grep -rnE --include='*.tf' \
  '^[[:space:]]*resource[[:space:]]+"(random_password|random_string|tls_private_key)"' "$dir" || true)
if [ -n "$stored" ]; then
  echo "error: generated secrets must be ephemeral resources, not stored ones:" >&2
  echo "$stored" >&2
  status=1
fi

# Inside these resource blocks, the secret-carrying attributes must be the
# write-only ones. Blocks start at a top-level `resource "<type>"` line and
# end at the next top-level `}`.
plain=$(find "$dir" -name '*.tf' -type f -print | sort | while IFS= read -r file; do
  awk -v file="$file" '
    /^resource[[:space:]]+"/ {
      type = $2
      gsub(/"/, "", type)
      watched = (type == "aws_secretsmanager_secret_version" || type == "aws_ssm_parameter" || type == "aws_db_instance")
      next
    }
    /^}/ { watched = 0; next }
    watched && /^[[:space:]]*(secret_string|secret_binary|value|password)[[:space:]]*=/ {
      print file ":" FNR ": " $0
    }
  ' "$file"
done)
if [ -n "$plain" ]; then
  echo "error: secrets must be written with write-only attributes (*_wo), which never enter state:" >&2
  echo "$plain" >&2
  status=1
fi

if [ "$status" -eq 0 ]; then
  echo "ok: every generated secret under $dir is ephemeral and written write-only"
fi
exit "$status"
