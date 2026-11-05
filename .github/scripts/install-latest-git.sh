#!/usr/bin/env bash
# Puts the latest stable Git release on PATH for the required interop smoke
# (.github/workflows/ci.yml, job interop-smoke), and refuses to let the job
# test any other Git.
#
# usage: install-latest-git.sh install | build | check
#
#   install  Resolves the latest stable release from kernel.org (falling back
#            to the tags of github.com/git/git) and exports it as
#            ASPEN_GIT_LATEST. Uses, in order, the first source that yields
#            exactly that version:
#              1. the Git already on the runner image;
#              2. the git-core PPA (Ubuntu Git Maintainers), read straight from
#                 its package host with the PPA key pinned in keys/, so the
#                 Launchpad API that add-apt-repository needs is never called;
#              3. a build from the release tarball on kernel.org (step outputs
#                 build=true, prefix and cache-key, and prefix/bin goes on
#                 PATH; the workflow restores the build from the Actions cache
#                 or runs `build`).
#            Network steps retry with backoff. Writes GITHUB_ENV, GITHUB_PATH
#            and GITHUB_OUTPUT when they are set.
#   build    Downloads git-$ASPEN_GIT_LATEST.tar.xz and its detached signature
#            from kernel.org, verifies the signature against Junio C Hamano's
#            release key (keys/git-release-signing.asc, primary fingerprint
#            below), and builds and installs it under the prefix.
#   check    Fails unless `git --version` on PATH is exactly
#            "git version $ASPEN_GIT_LATEST".
#
# Testing the fallbacks: ASPEN_GIT_PPA_URL replaces the PPA's package host.
# On a runner whose Git is older than the latest release, pointing it at an
# address that fails exercises source 3.
#
# Keys: keys/git-core-ppa.asc holds the PPA's two signing keys (Launchpad
# signs with E1DD2702..., and lists F911AB18... as the PPA's key); refresh
# them from keyserver.ubuntu.com if the PPA rotates keys. The signing subkeys
# of keys/git-release-signing.asc expire on 2028-01-11; refresh it from
# https://git.kernel.org/pub/scm/docs/kernel/pgpkeys.git/plain/keys/20D04E5A713660A7.asc
# when Junio extends them.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
readonly here
readonly release_key_fpr=96E07AF25771955980DAD10020D04E5A713660A7
readonly kernel_org_mirrors=(https://mirrors.edge.kernel.org https://cdn.kernel.org)
readonly kernel_org_path=/pub/software/scm/git
readonly ppa_url=${ASPEN_GIT_PPA_URL:-https://ppa.launchpadcontent.net/git-core/ppa/ubuntu}
readonly attempts=3

log() { printf '%s\n' "$*" >&2; }
die() { printf '::error::%s\n' "$*" >&2; exit 1; }

# retry <description> <command...>: up to $attempts tries, waiting 5 s, then
# 15 s, between them.
retry() {
  local what=$1 try=1 delay=5
  shift
  until "$@"; do
    if ((try >= attempts)); then
      log "$what failed $try times; giving up"
      return 1
    fi
    log "$what failed (try $try of $attempts); retrying in ${delay}s"
    sleep "$delay"
    try=$((try + 1)) delay=$((delay * 3))
  done
}

fetch() { curl -fsSL --connect-timeout 10 --max-time 300 "$@"; }

is_release() { [[ $1 =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; }

latest_from_kernel_org() {
  local mirror listing version
  for mirror in "${kernel_org_mirrors[@]}"; do
    listing=$(fetch "$mirror$kernel_org_path/") || continue
    version=$(grep -oE 'git-[0-9]+\.[0-9]+\.[0-9]+\.tar\.xz' <<<"$listing" \
      | sed 's/^git-//; s/\.tar\.xz$//' | sort -uV | tail -n 1)
    is_release "$version" && { echo "$version"; return; }
  done
  return 1
}

latest_from_github() {
  local tags version
  tags=$(git ls-remote --tags --refs https://github.com/git/git 'refs/tags/v*') || return 1
  version=$(sed -nE 's#^.*refs/tags/v([0-9]+\.[0-9]+\.[0-9]+)$#\1#p' <<<"$tags" | sort -V | tail -n 1)
  is_release "$version" && echo "$version"
}

resolve_latest() {
  local version
  if version=$(retry "listing Git releases on kernel.org" latest_from_kernel_org); then
    log "latest stable Git on kernel.org: $version"
  elif version=$(retry "listing Git tags on github.com/git/git" latest_from_github); then
    log "latest stable Git on github.com/git/git (kernel.org unavailable): $version"
  else
    die "cannot determine the latest stable Git release (kernel.org and github.com/git/git both failed)"
  fi
  echo "$version"
}

git_version() { { git --version 2>/dev/null || true; } | sed -n 's/^git version //p'; }

# Git records its install prefix at build time, so a cached build must be
# restored to the same absolute path.
prefix_for() { echo "$HOME/.cache/aspen-git/$1"; }

cache_key_for() {
  local image=${ImageOS:-}
  # shellcheck source=/dev/null
  [[ -n $image ]] || image=$(. /etc/os-release && echo "$ID-$VERSION_ID")
  # The script's hash invalidates the cache when the build recipe changes.
  echo "aspen-git-$1-$image-$(uname -m)-$(sha256sum "${BASH_SOURCE[0]}" | cut -c1-16)"
}

set_output() { echo "$1=$2" >>"${GITHUB_OUTPUT:-/dev/null}"; }

ppa_install() {
  local codename list=/etc/apt/sources.list.d/aspen-git-core-ppa.list
  local key=/etc/apt/keyrings/aspen-git-core-ppa.asc
  # shellcheck source=/dev/null
  codename=$(. /etc/os-release && echo "$VERSION_CODENAME")
  # Drop any entry the runner image already has for this PPA: apt refuses two
  # entries for one archive with different signed-by keys.
  grep -rlsE 'ppa\.launchpad(content)?\.net/git-core/ppa' /etc/apt/sources.list.d \
    | grep -vxF "$list" | xargs -r sudo rm -f -- || true
  sudo install -D -m 644 "$here/keys/git-core-ppa.asc" "$key" || return 1
  echo "deb [signed-by=$key] $ppa_url $codename main" | sudo tee "$list" >/dev/null || return 1
  # apt-get update reports an unreachable source only as a warning, and can
  # fail because of another source; the version check after the install is
  # what decides.
  retry "apt-get update" sudo apt-get update -q || true
  retry "apt-get install git" sudo apt-get install -yq --no-install-recommends git
}

cmd_install() {
  local latest have
  latest=$(resolve_latest)
  echo "ASPEN_GIT_LATEST=$latest" >>"${GITHUB_ENV:-/dev/null}"
  set_output version "$latest"

  have=$(git_version)
  if [[ $have == "$latest" ]]; then
    log "the runner's Git ($(command -v git)) is already $latest"
    set_output build false
    return
  fi
  log "the runner's Git is ${have:-missing}; trying the git-core PPA at $ppa_url"

  if ppa_install && have=$(git_version) && [[ $have == "$latest" ]]; then
    log "git on PATH is now $latest, from the git-core PPA"
    set_output build false
    return
  fi
  log "the git-core PPA did not provide Git $latest (have ${have:-none}); falling back to a source build"

  local prefix
  prefix=$(prefix_for "$latest")
  echo "$prefix/bin" >>"${GITHUB_PATH:-/dev/null}"
  set_output build true
  set_output prefix "$prefix"
  set_output cache-key "$(cache_key_for "$latest")"
}

# Prints a binary OpenPGP keyring for gpgv from an ASCII-armored key file.
dearmor() { sed '1,/^$/d; /^=/d; /^-----END/d' "$1" | base64 -d; }

download_release() {
  local version=$1 dir=$2 mirror
  for mirror in "${kernel_org_mirrors[@]}"; do
    if fetch -o "$dir/git.tar.xz" "$mirror$kernel_org_path/git-$version.tar.xz" \
      && fetch -o "$dir/git.tar.sign" "$mirror$kernel_org_path/git-$version.tar.sign"; then
      log "downloaded git-$version.tar.xz from $mirror"
      return
    fi
  done
  return 1
}

# The signature covers the uncompressed tar.
verify_release() {
  local dir=$1 status
  dearmor "$here/keys/git-release-signing.asc" >"$dir/release.gpg"
  status=$(xz -dc "$dir/git.tar.xz" \
    | gpgv --status-fd 1 --keyring "$dir/release.gpg" "$dir/git.tar.sign" - 2>/dev/null) \
    || die "the signature on git.tar.xz does not verify against $release_key_fpr"
  # VALIDSIG <signing key fpr> ... <primary key fpr>
  awk -v fpr="$release_key_fpr" '$2 == "VALIDSIG" && $NF == fpr { ok = 1 } END { exit !ok }' <<<"$status" \
    || die "git.tar.xz is not signed by $release_key_fpr"
  log "signature verified: signed by $release_key_fpr"
}

have_header() { echo "#include <$1>" | cc -E -x c - >/dev/null 2>&1; }

ensure_build_deps() {
  local missing=() pair
  for pair in curl/curl.h:libcurl4-openssl-dev zlib.h:zlib1g-dev expat.h:libexpat1-dev; do
    have_header "${pair%%:*}" || missing+=("${pair#*:}")
  done
  if ((${#missing[@]})); then
    retry "apt-get install ${missing[*]}" sh -c \
      'sudo apt-get install -yq --no-install-recommends "$@" || { sudo apt-get update -q; false; }' \
      sh "${missing[@]}"
  fi
  # Git 2.55 and later build their Rust code by default; testkit/images/git
  # explains why this does not use NO_RUST.
  command -v cargo >/dev/null || die "cargo is not on PATH; Git 2.55 and later need it to build"
}

cmd_build() {
  local version=${ASPEN_GIT_LATEST:?run install first} prefix
  is_release "$version" || die "ASPEN_GIT_LATEST=$version is not a release version"
  prefix=$(prefix_for "$version")
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  ensure_build_deps
  retry "downloading git-$version from kernel.org" download_release "$version" "$work" \
    || die "cannot download git-$version from kernel.org"
  verify_release "$work"
  mkdir "$work/src"
  tar -xJf "$work/git.tar.xz" -C "$work/src" --strip-components=1
  log "building Git $version into $prefix"
  # Same options as testkit/images/git: no GUI, translations, Perl or Python
  # commands, and no OpenSSL (HTTPS gets its TLS from libcurl).
  make -C "$work/src" -j"$(nproc)" prefix="$prefix" \
    NO_TCLTK=1 NO_GETTEXT=1 NO_PERL=1 NO_PYTHON=1 NO_OPENSSL=1 INSTALL_SYMLINKS=1 INSTALL_STRIP=-s \
    install >"$work/build.log" 2>&1 || { tail -n 50 "$work/build.log" >&2; die "building Git $version failed"; }
  [[ $("$prefix/bin/git" --version) == "git version $version" ]] \
    || die "the build in $prefix does not report version $version"
}

cmd_check() {
  local want=${ASPEN_GIT_LATEST:?run install first} have
  have=$(git_version)
  log "git on PATH: $(command -v git || echo none) ($(git --version 2>&1 || true))"
  [[ $have == "$want" ]] \
    || die "the interop smoke must run against the latest stable Git $want, but git on PATH is ${have:-missing}"
  log "Git $want is on PATH"
}

case ${1:-} in
  install) cmd_install ;;
  build) cmd_build ;;
  check) cmd_check ;;
  *) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2; exit 2 ;;
esac
