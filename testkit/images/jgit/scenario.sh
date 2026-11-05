#!/bin/sh
# Client-matrix scenario for JGit, through its command-line front end
# (`org.eclipse.jgit.pgm`). Same steps and output as ../git/scenario.sh,
# minus `fsck` (JGit's CLI has none; run-matrix.sh runs it with stock Git).
#
# usage: scenario.sh <url> <branch> [<branch-url>]     (needs `jgit` on PATH)
#
# <branch-url> is the URL to clone <branch> through (default <url>).
set -eu
url=$1
branch=$2
branch_url=${3:-$1}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Ignore the caller's Git environment; HOME is the scratch directory.
unset GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PROTOCOL

# JGit reads ~/.gitconfig from Java's user.home, not from $HOME.
export HOME="$work" GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0
export JAVA_TOOL_OPTIONS="-Duser.home=$work"
cat > "$work/.gitconfig" <<CONFIG
[user]
	name = Aspen Matrix
	email = matrix@aspen.invalid
[protocol]
	version = 2
CONFIG

echo "client $(jgit --version 2>/dev/null)"
cd "$work"
jgit clone "$url" a >&2
cd a
echo "cloned $(jgit rev-parse HEAD 2>/dev/null)"

echo "pushed by JGit" > matrix.txt
jgit add matrix.txt >&2
jgit commit -m "matrix: $branch" >&2
head=$(jgit rev-parse HEAD 2>/dev/null)
jgit push origin "refs/heads/main:refs/heads/$branch" >&2
echo "pushed $head"

cd "$work"
jgit clone --branch "$branch" "$branch_url" b >&2
cd b
echo "fetched $(jgit rev-parse HEAD 2>/dev/null)"

cd "$work/a"
jgit fetch origin >&2
