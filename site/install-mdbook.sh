#!/usr/bin/env bash
# Install the pinned mdBook release binary (Linux x86_64) into a directory on
# PATH, verifying its checksum. Used by CI; elsewhere, run
# `cargo install mdbook --version <version> --locked`.
#
#   site/install-mdbook.sh [dir]   # default: $HOME/.local/bin
set -euo pipefail

version=0.5.4
sha256=3f28de05dafca9d0f2eab99c662116b0e37b89b1d96a08f8f430b9eeae958cd7
dir=${1:-$HOME/.local/bin}

case "$(uname -sm)" in
  "Linux x86_64") ;;
  *) echo "install-mdbook.sh: only Linux x86_64; run: cargo install mdbook --version $version --locked" >&2; exit 1 ;;
esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
archive="mdbook-v$version-x86_64-unknown-linux-gnu.tar.gz"
curl -fsSL -o "$tmp/$archive" "https://github.com/rust-lang/mdBook/releases/download/v$version/$archive"
echo "$sha256  $tmp/$archive" | sha256sum -c --quiet -
mkdir -p "$dir"
tar -xzf "$tmp/$archive" -C "$dir" mdbook
"$dir/mdbook" --version
