#!/bin/sh
# Client-matrix scenario for gitoxide, through its `gix` command-line tool.
#
# usage: scenario.sh <url> <branch> [<branch-url>]     (needs `gix` on PATH)
#
# gitoxide cannot push yet, so this scenario prints `push-unsupported`,
# clones the `next` branch that run-matrix.sh seeds instead of a pushed
# branch, and fetches. Output lines are those of ../git/scenario.sh.
# <branch-url> is ignored: `gix` fetches with protocol v2, whose `ls-refs`
# lists `next` on the repository URL of every server, Aspen included.
set -eu
url=$1
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Ignore the caller's Git environment; HOME is the scratch directory.
unset GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PROTOCOL
export HOME="$work" GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0

echo "client $(gix --version)"
gix clone "$url" "$work/a" >&2
echo "cloned $(gix -r "$work/a" revision resolve HEAD)"
echo "push-unsupported"
gix clone --ref next "$url" "$work/b" >&2
gix -r "$work/b" verify >&2
echo "fetched $(gix -r "$work/b" revision resolve HEAD)"
gix -r "$work/a" fetch >&2
