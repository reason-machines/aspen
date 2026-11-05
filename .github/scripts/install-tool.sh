#!/usr/bin/env bash
# Installs a pinned command-line tool for CI, after checking its download
# against a pinned SHA-256.
#
#   install-tool.sh <tool> <dir>
#
# Puts the tool's executable at <dir>/<tool>. The tools, their versions, URLs
# and hashes are the table below, so every workflow installs the same bytes
# and a moved or replaced release asset fails the job instead of running
# (issue #122). Workflows must not download tools with curl or wget
# themselves; `cargo xtask check-workflows` refuses that.
#
# To bump a tool: change its version, URL and hash together, taking the hash
# from the project's published checksum file and checking it against a fresh
# download (`sha256sum`).
set -euo pipefail

die() { echo "install-tool: $*" >&2; exit 1; }

[[ $# -eq 2 ]] || die "usage: install-tool.sh <helm|kubeconform|kind> <dir>"
tool=$1 dir=$2

# url, sha256 of the download, and the executable's path inside the archive
# (empty for a bare executable).
case $tool in
  helm)
    url=https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz
    sha256=a7f81ce08007091b86d8bd696eb4d86b8d0f2e1b9f6c714be62f82f96a594496
    member=linux-amd64/helm
    ;;
  kubeconform)
    url=https://github.com/yannh/kubeconform/releases/download/v0.7.0/kubeconform-linux-amd64.tar.gz
    sha256=c31518ddd122663b3f3aa874cfe8178cb0988de944f29c74a0b9260920d115d3
    member=kubeconform
    ;;
  kind)
    url=https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-amd64
    sha256=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
    member=
    ;;
  *) die "unknown tool '$tool' (known: helm, kubeconform, kind)" ;;
esac

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -fsSL --retry 3 --connect-timeout 10 --max-time 300 -o "$work/download" "$url" \
  || die "cannot download $url"
actual=$(sha256sum "$work/download" | cut -d' ' -f1)
[[ $actual == "$sha256" ]] \
  || die "$url has SHA-256 $actual, expected $sha256; refusing to install it"

mkdir -p "$dir"
if [[ -n $member ]]; then
  tar -xzf "$work/download" -C "$work" "$member"
  install -m 0755 "$work/$member" "$dir/$tool"
else
  install -m 0755 "$work/download" "$dir/$tool"
fi
echo "install-tool: $tool from $url (sha256 $sha256) at $dir/$tool"
