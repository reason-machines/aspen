#!/usr/bin/env bash
# Build one git client version from source into $GITS_DIR/<name>.
#
#   build-git.sh <name> <git-ref> [src-dir]
#
# Examples:
#   build-git.sh 2.30 v2.30.9
#   build-git.sh openai openai/codex
#
# Needs a git.git clone in $SRC (default /tmp/gitsrc, remote "openai" added
# for github.com/openai/git) and headers for curl, zlib, openssl and expat.
# If the headers live in a non-system prefix, set SYSROOT (we extract the
# Debian -dev packages there when we have no root).
set -euo pipefail

name=$1
ref=$2
SRC=${3:-${SRC:-/tmp/gitsrc}}
GITS_DIR=${GITS_DIR:-/tmp/gits}
SYSROOT=${SYSROOT:-}

prefix="$GITS_DIR/$name"
work="$GITS_DIR/.build/$name"

if [ -x "$prefix/bin/git" ]; then
  echo "$name already built: $("$prefix/bin/git" --version)"
  exit 0
fi

mkdir -p "$GITS_DIR/.build"
rm -rf "$work"
git -C "$SRC" worktree add -q -f --detach "$work" "$ref" >/dev/null

cflags="-O1 -g0"
ldflags=""
if [ -n "$SYSROOT" ]; then
  cflags="$cflags -I$SYSROOT/usr/include -I$SYSROOT/usr/include/x86_64-linux-gnu"
  ldflags="-L$SYSROOT/usr/lib/x86_64-linux-gnu"
fi

# Old releases (2.30) trip newer compilers' default warnings-as-errors and
# multiple-definition checks; -fcommon and DEVELOPER unset keep them building.
make -C "$work" -j"$(nproc)" \
  prefix="$prefix" \
  CFLAGS="$cflags -fcommon" LDFLAGS="$ldflags" CURL_LDFLAGS="-lcurl" \
  NO_GETTEXT=1 NO_TCLTK=1 NO_PERL=1 NO_PYTHON=1 NO_INSTALL_HARDLINKS=1 \
  install >"$GITS_DIR/.build/$name.log" 2>&1 || {
    tail -40 "$GITS_DIR/.build/$name.log"
    exit 1
  }

git -C "$SRC" worktree remove --force "$work"
echo "$name: $("$prefix/bin/git" --version)"
