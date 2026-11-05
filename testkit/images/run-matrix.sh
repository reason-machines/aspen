#!/usr/bin/env bash
# Runs the client-matrix scenario through each Git client against a Git HTTP
# server, and checks the result with stock Git.
#
# usage: testkit/images/run-matrix.sh [--native] [--no-build] [--url BASE | --aspen BIN] [CLIENT...]
#
# Clients (default: all):
#   git-2.30 git-2.39 git-2.43 git-2.47 git-latest jgit libgit2 go-git gitoxide
#
# By default each client runs in its Docker image (aspen-matrix-<client>, built
# from testkit/images/<dir>/Dockerfile unless --no-build) with host networking.
# --native runs the same scenario files with clients already on this machine
# instead (git, jgit, python3 with pygit2, Go or $GO_GIT_SCENARIO, gix). A
# git-* client then uses $GIT_BUILDS/git-<version>/bin/git when GIT_BUILDS
# points at a directory of `make prefix=$GIT_BUILDS/git-<version> install`
# builds, and otherwise the git on PATH.
#
# The server is `aspen-testkit serve` (stock `git http-backend`) on loopback,
# unless --url points at another server's base URL; that server must serve
# a repository `matrix.git` that this script can seed through `git push`.
# --aspen BIN runs a real Aspen instead: `BIN dev` (the `aspen` binary;
# SQLite and a local blob store) on loopback, with a `matrix` repository.
# Aspen's repository URL advertises only trunk (spike S2), so clients that
# fetch with protocol v0 (libgit2, go-git) clone a pushed branch through its
# branch-scoped URL, `<url>/-/refs/heads/<branch>`.
#
# Scenario contract: each client's image (or native command) takes
# `<url> <branch> [<branch-url>]` (the URL to clone <branch> through; the
# repository URL when absent) and prints `client <version>`, `cloned <oid>`,
# `pushed <oid>` or `push-unsupported`, and `fetched <oid>`; see
# testkit/images/git/scenario.sh. This script then checks those against the
# server with stock Git, including `git fsck --strict` on a fresh clone.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$here/../.." && pwd)
all_clients=(git-2.30 git-2.39 git-2.43 git-2.47 git-latest jgit libgit2 go-git gitoxide)

# Pinned Git releases; git-latest is resolved from kernel.org at build time
# unless GIT_LATEST is set.
declare -A git_versions=(
  [git-2.30]=2.30.9
  [git-2.39]=2.39.5
  [git-2.43]=2.43.7
  [git-2.47]=2.47.3
)

native=0
build=1
base_url=
aspen=
clients=()
while (($#)); do
  case $1 in
    --native) native=1 ;;
    --no-build) build=0 ;;
    --url) base_url=$2; shift ;;
    --aspen) aspen=$2; shift ;;
    -h|--help) sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown option $1" >&2; exit 2 ;;
    *) clients+=("$1") ;;
  esac
  shift
done
((${#clients[@]})) || clients=("${all_clients[@]}")
if [[ -n $aspen && -n $base_url ]]; then
  echo "--aspen and --url are exclusive" >&2; exit 2
fi

work=$(mktemp -d)
server_pid=
cleanup() {
  [[ -n $server_pid ]] && kill "$server_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

# Stock Git for seeding and checking, isolated from the machine's config.
hgit() {
  HOME="$work/home" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 \
    GIT_AUTHOR_NAME="Aspen Matrix" GIT_AUTHOR_EMAIL=matrix@aspen.invalid \
    GIT_COMMITTER_NAME="Aspen Matrix" GIT_COMMITTER_EMAIL=matrix@aspen.invalid \
    git "$@"
}
mkdir -p "$work/home"

latest_git_version() {
  if [[ -n ${GIT_LATEST:-} ]]; then echo "$GIT_LATEST"; return; fi
  curl -fsSL https://mirrors.edge.kernel.org/pub/software/scm/git/ \
    | grep -oE 'git-[0-9]+\.[0-9]+\.[0-9]+\.tar\.xz' | sed 's/^git-//; s/\.tar\.xz$//' \
    | sort -uV | tail -1
}

git_version_of() {
  if [[ $1 == git-latest ]]; then latest_git_version; else echo "${git_versions[$1]}"; fi
}

image_dir() {
  case $1 in
    git-*) echo git ;;
    jgit|libgit2|go-git|gitoxide) echo "$1" ;;
    *) echo "unknown client $1 (known: ${all_clients[*]})" >&2; return 1 ;;
  esac
}

build_image() {
  local client=$1 dir args=()
  dir=$(image_dir "$client")
  case $client in
    git-*) args=(--build-arg "GIT_VERSION=$(git_version_of "$client")") ;;
  esac
  echo "== building aspen-matrix-$client" >&2
  docker build --quiet "${args[@]}" -t "aspen-matrix-$client" "$here/$dir" >&2
}

run_client() {
  local client=$1 url=$2 branch=$3 branch_url=$4
  if ((native)); then
    case $client in
      git-*)
        local version prefix
        version=$(git_version_of "$client")
        prefix="${GIT_BUILDS:-/nonexistent}/git-$version/bin"
        if [[ -x $prefix/git ]]; then
          PATH="$prefix:$PATH" "$here/git/scenario.sh" "$url" "$branch" "$branch_url"
        else
          "$here/git/scenario.sh" "$url" "$branch" "$branch_url"
        fi
        ;;
      jgit) "$here/jgit/scenario.sh" "$url" "$branch" "$branch_url" ;;
      libgit2) "${PYTHON:-python3}" "$here/libgit2/scenario.py" "$url" "$branch" "$branch_url" ;;
      go-git)
        if [[ -n ${GO_GIT_SCENARIO:-} ]]; then
          "$GO_GIT_SCENARIO" "$url" "$branch" "$branch_url"
        else
          (cd "$here/go-git" && go run . "$url" "$branch" "$branch_url")
        fi
        ;;
      gitoxide) "$here/gitoxide/scenario.sh" "$url" "$branch" "$branch_url" ;;
    esac
  else
    docker run --rm --network host "aspen-matrix-$client" "$url" "$branch" "$branch_url"
  fi
}

# --- server and seed repository ---------------------------------------------
wait_for_listening() {
  for _ in $(seq 300); do
    base_url=$(sed -n 's/.*listening on \(http:\/\/[^ ]*\).*/\1/p' "$work/serve.log" | head -1)
    [[ -n $base_url ]] && return
    kill -0 "$server_pid" 2>/dev/null || break
    sleep 0.1
  done
  echo "server did not start:" >&2; cat "$work/serve.log" >&2; exit 1
}

if [[ -n $aspen ]]; then
  [[ -x $aspen ]] || { echo "--aspen $aspen: not an executable" >&2; exit 2; }
  "$aspen" dev --listen 127.0.0.1:0 --data-dir "$work/aspen" --repo matrix >"$work/serve.log" 2>&1 &
  server_pid=$!
  wait_for_listening
elif [[ -z $base_url ]]; then
  testkit=${ASPEN_TESTKIT:-}
  if [[ -z $testkit ]]; then
    (cd "$repo_root" && cargo build --quiet -p aspen-testkit)
    testkit="$repo_root/target/debug/aspen-testkit"
  fi
  mkdir -p "$work/served"
  hgit init --quiet --bare --initial-branch=main "$work/served/matrix.git"
  hgit -C "$work/served/matrix.git" config http.receivepack true
  "$testkit" serve --root "$work/served" --addr 127.0.0.1:0 >"$work/serve.log" 2>&1 &
  server_pid=$!
  wait_for_listening
fi
url="$base_url/matrix.git"
echo "== server: $url${aspen:+ (aspen dev: $("$aspen" --version))}"

# The URL to clone a branch through: Aspen's branch-scoped URL, or the
# repository URL for servers that show every branch on it.
branch_url_of() {
  if [[ -n $aspen ]]; then echo "$url/-/refs/heads/$1"; else echo "$url"; fi
}

# main = one commit; next = main plus one commit (for clients that cannot push).
hgit init --quiet --initial-branch=main "$work/seed"
echo "seeded by run-matrix.sh" >"$work/seed/README"
hgit -C "$work/seed" add README
hgit -C "$work/seed" commit --quiet -m "matrix: seed"
main_tip=$(hgit -C "$work/seed" rev-parse HEAD)
echo "next" >"$work/seed/NEXT"
hgit -C "$work/seed" add NEXT
hgit -C "$work/seed" commit --quiet -m "matrix: next"
next_tip=$(hgit -C "$work/seed" rev-parse HEAD)
hgit -C "$work/seed" push --quiet "$url" "$main_tip:refs/heads/main" "$next_tip:refs/heads/next"

# --- run the matrix ---------------------------------------------------------
failures=0
summary=()
for client in "${clients[@]}"; do
  image_dir "$client" >/dev/null
  label=$client
  ((native)) && label="$client (native)"
  if ((!native && build)); then
    if ! build_image "$client"; then
      summary+=("FAIL $label: image build failed"); failures=$((failures + 1)); continue
    fi
  fi
  branch="matrix/$client"
  echo "== $label"
  if ! out=$(run_client "$client" "$url" "$branch" "$(branch_url_of "$branch")" 2>"$work/stderr"); then
    sed 's/^/   | /' "$work/stderr"
    summary+=("FAIL $label: scenario exited non-zero"); failures=$((failures + 1)); continue
  fi
  echo "$out" | sed 's/^/   /'
  field() { echo "$out" | sed -n "s/^$1 //p" | head -1; }
  version=$(field client)
  cloned=$(field cloned)
  pushed=$(field pushed)
  fetched=$(field fetched)
  problems=()
  [[ $cloned == "$main_tip" ]] || problems+=("cloned ${cloned:-nothing}, expected main at $main_tip")
  if echo "$out" | grep -qx push-unsupported; then
    [[ $fetched == "$next_tip" ]] || problems+=("fetched ${fetched:-nothing}, expected next at $next_tip")
  else
    remote=$(hgit ls-remote "$url" "refs/heads/$branch" | cut -f1)
    [[ -n $pushed && $remote == "$pushed" ]] || problems+=("server has $branch at ${remote:-nothing}, client pushed ${pushed:-nothing}")
    [[ $fetched == "$pushed" ]] || problems+=("fetched ${fetched:-nothing}, expected the pushed $pushed")
  fi
  check="$work/check-$client"
  if hgit clone --quiet --mirror "$url" "$check" 2>"$work/stderr"; then
    if [[ -n $pushed ]]; then
      parent=$(hgit -C "$check" rev-parse --verify --quiet "$pushed^" || true)
      [[ $parent == "$main_tip" ]] || problems+=("pushed commit's parent is ${parent:-missing}, expected $main_tip")
    fi
    hgit -C "$check" fsck --strict --no-progress >/dev/null 2>"$work/stderr" \
      || problems+=("git fsck --strict failed: $(head -3 "$work/stderr" | tr '\n' ' ')")
  else
    problems+=("fresh clone failed: $(head -3 "$work/stderr" | tr '\n' ' ')")
  fi
  rm -rf "$check"
  if ((${#problems[@]})); then
    joined=$(printf '%s; ' "${problems[@]}")
    summary+=("FAIL $label ($version): ${joined%; }"); failures=$((failures + 1))
  else
    summary+=("PASS $label ($version)")
  fi
done

echo
echo "== client matrix against $url${aspen:+ (aspen)}"
printf '%s\n' "${summary[@]}"
if ((failures)) && [[ -n $aspen ]]; then
  echo; echo "== aspen log (last 40 lines)"; tail -40 "$work/serve.log"
fi
((failures == 0))
