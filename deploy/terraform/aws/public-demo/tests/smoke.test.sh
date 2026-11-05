#!/bin/sh
# Runs smoke.sh with ASPEN_TOKEN_FILE against a local repository, with a
# `git` first on PATH that records every call's arguments and then runs the
# real git, and fails if the token, or the Basic credential made from it,
# ever appears in git's arguments: `ps` and /proc/<pid>/cmdline show those
# to every user on the host (issue #145).
#
#   deploy/terraform/aws/public-demo/tests/smoke.test.sh
#
# Needs bash, git 2.31 or later and base64. Nothing leaves the machine:
# ASPEN_URL is http://aspen.invalid, git rewrites it to a file:// repository
# (url.<base>.insteadOf), and a `curl` shim answers the health checks.
# Besides argv, it checks that the header is still in effect for the Aspen
# URL in the calls that clone and push, that it is not in effect for a CDN
# URL, that git calls outside git_aspen do not get it, and that smoke.sh
# refuses a token with a git older than 2.31 instead of pushing anonymously.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
script="$here/../smoke.sh"

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

aspen_url=http://aspen.invalid
token=aspen_0123456789abcdef0123456789abcdef_test-token-not-a-secret
printf '%s\n' "$token" >"$dir/token"
basic=$(printf 'smoke:%s' "$token" | base64 | tr -d '\n')

# Git sees only this test's configuration.
mkdir "$dir/home"
export HOME="$dir/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL="$dir/gitconfig"
unset GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS
cat >"$GIT_CONFIG_GLOBAL" <<EOF
[init]
	defaultBranch = main
[url "file://$dir/remote/"]
	insteadOf = $aspen_url/
EOF

# The repository smoke.sh pushes to and clones, with one commit on main.
real_git=$(command -v git)
"$real_git" init -q --bare "$dir/remote/smoke/check.git"
"$real_git" init -q "$dir/seed"
"$real_git" -C "$dir/seed" -c user.name=seed -c user.email=seed@example.invalid \
  commit -q --allow-empty -m seed
"$real_git" -C "$dir/seed" push -q "$dir/remote/smoke/check.git" HEAD:refs/heads/main

# The git shim appends, per call, its arguments (one per line) to git-argv,
# and to git-calls one line "<subcommand> <header>", where <header> says
# whether an Authorization header is in effect for the Aspen repository's
# URL ("header" or "none"), and "cdn-leak" is added when one is in effect
# for a CDN URL.
mkdir "$dir/shim"
cat >"$dir/shim/git" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >>"$dir/git-argv"
sub=
skip=
for arg in "\$@"; do
  if [ -n "\$skip" ]; then skip=; continue; fi
  case \$arg in
    -c|-C) skip=1 ;;
    -*) ;;
    *) sub=\$arg; break ;;
  esac
done
if "$real_git" config --get-urlmatch http.extraHeader "$aspen_url/smoke/check.git" 2>/dev/null |
    grep -q '^Authorization: Basic '; then
  header=header
else
  header=none
fi
if "$real_git" config --get-urlmatch http.extraHeader https://cdn.invalid/-/express/x >/dev/null 2>&1; then
  header="\$header cdn-leak"
fi
printf '%s %s\n' "\${sub:-?}" "\$header" >>"$dir/git-calls"
exec "$real_git" "\$@"
EOF
# The health checks: every URL answers 200.
cat >"$dir/shim/curl" <<'EOF'
#!/bin/sh
printf 200
EOF
chmod +x "$dir/shim/git" "$dir/shim/curl"

: >"$dir/git-argv"
: >"$dir/git-calls"
if ! PATH="$dir/shim:$PATH" ASPEN_URL="$aspen_url" SMOKE_REPO=smoke/check \
    ASPEN_TOKEN_FILE="$dir/token" bash "$script" >"$dir/out" 2>&1; then
  cat "$dir/out" >&2
  fail "smoke.sh did not pass against the local repository"
fi
grep -q '^all checks passed$' "$dir/out" || { cat "$dir/out" >&2; fail "smoke.sh did not report all checks passed"; }

[ -s "$dir/git-argv" ] || fail "smoke.sh did not run git through the test's PATH"
if grep -qF -e "$token" -e "$basic" "$dir/git-argv"; then
  fail "the token is on git's command line"
fi
if grep -qi 'extraHeader' "$dir/git-argv"; then
  fail "an extraHeader setting is on git's command line"
fi

# The header must still reach the calls that talk to Aspen: both clones and
# the push.
[ "$(grep -c '^clone header' "$dir/git-calls")" -ge 2 ] || fail "a clone ran without the Authorization header"
grep -q '^push header' "$dir/git-calls" || fail "the push ran without the Authorization header"
if grep -q 'cdn-leak' "$dir/git-calls"; then
  fail "the Authorization header is in effect for a CDN URL"
fi
if grep -Eq '^(add|rev-parse|symbolic-ref|fsck) header' "$dir/git-calls"; then
  fail "a git call outside git_aspen got the Authorization header"
fi
"$real_git" -C "$dir/remote/smoke/check.git" log -1 --format=%s main | grep -q '^smoke ' ||
  fail "the smoke commit did not reach the repository"

# A git older than 2.31 ignores GIT_CONFIG_COUNT: smoke.sh must refuse it.
mkdir "$dir/oldgit"
cat >"$dir/oldgit/git" <<EOF
#!/bin/sh
if [ "\$1" = version ]; then echo 'git version 2.30.9'; exit 0; fi
exec "$real_git" "\$@"
EOF
cp "$dir/shim/curl" "$dir/oldgit/curl"
chmod +x "$dir/oldgit/git"
status=0
PATH="$dir/oldgit:$PATH" ASPEN_URL="$aspen_url" SMOKE_REPO=smoke/check \
  ASPEN_TOKEN_FILE="$dir/token" bash "$script" >"$dir/old-out" 2>&1 || status=$?
if [ "$status" -ne 2 ] || ! grep -q 'needs git 2.31 or later' "$dir/old-out"; then
  cat "$dir/old-out" >&2
  fail "smoke.sh did not refuse git 2.30 with a token (exit $status)"
fi

echo "smoke.sh: all checks passed"
