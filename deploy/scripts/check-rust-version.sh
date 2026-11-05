#!/usr/bin/env sh
# Fail when the container image would build Aspen with a different Rust
# toolchain than the rest of the project.
#
# The workspace pins its toolchain in rust-toolchain.toml (`channel`). The
# image's build stage picks its toolchain from `ARG RUST_VERSION` in
# deploy/docker/Dockerfile and sets RUSTUP_TOOLCHAIN to it, so the two must be
# kept equal by hand. This check makes drift a CI failure instead of a silent
# difference between what CI tests and what the image ships.
#
# Usage (from the repository root):
#
#   deploy/scripts/check-rust-version.sh [rust-toolchain.toml] [Dockerfile]
#
# The arguments default to the real files; they exist so the check itself can
# be tested against deliberately mismatched copies.
set -eu

toolchain_file=${1:-rust-toolchain.toml}
dockerfile=${2:-deploy/docker/Dockerfile}

for f in "$toolchain_file" "$dockerfile"; do
  if [ ! -f "$f" ]; then
    echo "error: $f not found (run this from the repository root)" >&2
    exit 2
  fi
done

# channel = "1.99.1"   (inside the [toolchain] table; the file has no other)
channel=$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$toolchain_file")
# ARG RUST_VERSION=1.99.1   (the global default, before the first FROM)
image=$(sed -n 's/^[[:space:]]*ARG[[:space:]][[:space:]]*RUST_VERSION=\([^[:space:]]*\).*/\1/p' "$dockerfile")

if [ -z "$channel" ] || [ "$(printf '%s\n' "$channel" | wc -l)" -ne 1 ]; then
  echo "error: expected exactly one 'channel = \"...\"' line in $toolchain_file" >&2
  exit 1
fi
if [ -z "$image" ] || [ "$(printf '%s\n' "$image" | wc -l)" -ne 1 ]; then
  echo "error: expected exactly one 'ARG RUST_VERSION=<version>' line in $dockerfile" >&2
  exit 1
fi

if [ "$channel" != "$image" ]; then
  {
    echo "error: Rust toolchain mismatch"
    printf '  %-26s channel      = "%s"\n' "$toolchain_file" "$channel"
    printf '  %-26s RUST_VERSION = %s\n' "$dockerfile" "$image"
  } >&2
  cat >&2 <<EOF
The container image must build with the toolchain the workspace pins.
Edit $dockerfile and set: ARG RUST_VERSION=$channel
(If you meant to change the toolchain, change $toolchain_file and the Dockerfile together.)
EOF
  exit 1
fi

echo "ok: $dockerfile RUST_VERSION matches $toolchain_file channel ($channel)"
