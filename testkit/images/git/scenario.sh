#!/bin/sh
# Client-matrix scenario for stock Git (any version).
#
# usage: scenario.sh <url> <branch> [<branch-url>]
#
# Clones <url>, commits one file, pushes it to refs/heads/<branch>, clones
# <branch> again (through <branch-url>, default <url>; Aspen's
# branch-scoped URL), then fetches into the first clone. Prints one line per
# result for run-matrix.sh to check:
#   client <description>
#   cloned <oid of the cloned HEAD>
#   pushed <oid pushed to <branch>>
#   fetched <oid of <branch> in the second clone>
set -eu
url=$1
branch=$2
branch_url=${3:-$1}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Ignore the caller's Git environment; HOME is the scratch directory.
unset GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PROTOCOL

export HOME="$work" GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0
git config --global user.name "Aspen Matrix"
git config --global user.email "matrix@aspen.invalid"
git config --global protocol.version 2
git config --global init.defaultBranch main

echo "client $(git --version)"
git clone --quiet "$url" "$work/a"
echo "cloned $(git -C "$work/a" rev-parse HEAD)"

echo "pushed by $(git --version)" > "$work/a/matrix.txt"
git -C "$work/a" add matrix.txt
git -C "$work/a" commit --quiet -m "matrix: $branch"
git -C "$work/a" push --quiet origin "HEAD:refs/heads/$branch"
echo "pushed $(git -C "$work/a" rev-parse HEAD)"

git clone --quiet --branch "$branch" "$branch_url" "$work/b"
git -C "$work/b" fsck --strict --no-progress
echo "fetched $(git -C "$work/b" rev-parse HEAD)"

# An incremental fetch that has to negotiate with what the clone already has.
git -C "$work/a" fetch --quiet origin
