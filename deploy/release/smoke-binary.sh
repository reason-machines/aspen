#!/usr/bin/env bash
# Smoke test of one built `aspen` binary, as a user would first run it:
# `aspen dev` on loopback, then a push and a clone with stock git, and a
# clean exit on SIGTERM. The release workflow runs it on every target
# before anything is packaged. Works with macOS's bash 3.2.
#
#   deploy/release/smoke-binary.sh path/to/aspen [expected-version]
set -euo pipefail

bin="${1:?usage: smoke-binary.sh <aspen binary> [version]}"
expected="${2:-}"
work="$(mktemp -d)"
pid=""
cleanup() {
  if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

version="$("$bin" --version)"
echo "$version"
if [ -n "$expected" ] && [ "$version" != "aspen $expected" ]; then
  echo "expected 'aspen $expected'" >&2
  exit 1
fi

port=$((20000 + RANDOM % 20000))
"$bin" dev --listen "127.0.0.1:$port" --data-dir "$work/data" 2>"$work/dev.log" &
pid=$!
ready=0
for _ in $(seq 1 60); do
  if "$bin" healthcheck --url "http://127.0.0.1:$port/readyz" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  cat "$work/dev.log" >&2
  exit 1
fi

g() {
  GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 \
    GIT_AUTHOR_NAME=smoke GIT_AUTHOR_EMAIL=smoke@example.com \
    GIT_COMMITTER_NAME=smoke GIT_COMMITTER_EMAIL=smoke@example.com git "$@"
}
g init -q "$work/src"
g -C "$work/src" checkout -q -b main
echo hello >"$work/src/hello.txt"
g -C "$work/src" add hello.txt
g -C "$work/src" commit -q -m hello
g -C "$work/src" push -q "http://127.0.0.1:$port/demo.git" main
g clone -q "http://127.0.0.1:$port/demo.git" "$work/clone"
[ "$(g -C "$work/clone" rev-parse HEAD)" = "$(g -C "$work/src" rev-parse HEAD)" ]
g -C "$work/clone" fsck --strict
echo "push, clone, fsck --strict: ok"

kill -TERM "$pid"
code=0
wait "$pid" || code=$?
pid=""
echo "aspen dev exited $code on SIGTERM"
[ "$code" = 0 ]
