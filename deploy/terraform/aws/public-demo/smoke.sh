#!/usr/bin/env bash
# Smoke test of a deployed Aspen public-demo environment (../README.md, and
# docs/PLAN.md section 6), from any machine with git, curl and bash. It
# never prints a credential.
#
#   ASPEN_URL=https://git.staging.aspen.example.com \
#   SMOKE_REPO=smoke/check ASPEN_TOKEN_FILE=~/.aspen-smoke-token \
#   CDN_URL=https://cdn.git.staging.aspen.example.com/-/express \
#   PLACE_URL=https://place.staging.aspen.example.com \
#     deploy/terraform/aws/public-demo/smoke.sh
#
# Every variable but ASPEN_URL is optional; each check runs when its inputs
# are set:
#
#   ASPEN_URL         Aspen's base URL: /healthz and /readyz
#   SMOKE_REPO        a repository to push a commit to, clone back and fsck
#   ASPEN_TOKEN_FILE  file holding a token with write on SMOKE_REPO (unset:
#                     push anonymously, as `aspen dev` allows); needs
#                     git 2.31 or later
#   CDN_URL           the CDN base Aspen signs under (<cdn>/-/express): an
#                     expired URL and a non-express path are refused at the
#                     edge, the health probe answers
#   PLACE_URL         the place page: page, API, clone, push a tile to a
#                     branch, see it on the canvas
#   SSH_HOST, SSH_KEY, SSH_KNOWN_HOSTS
#                     ls-remote SMOKE_REPO over SSH with the pinned host key
set -euo pipefail

: "${ASPEN_URL:?set ASPEN_URL to the base URL of Aspen}"
ASPEN_URL=${ASPEN_URL%/}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
stamp=$(date -u +%Y%m%dT%H%M%SZ)-$$
failures=0

pass() { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*" >&2; failures=$((failures + 1)); }

status_of() { curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$@"; }

# The token's Authorization header reaches git through GIT_CONFIG_COUNT,
# GIT_CONFIG_KEY_<n> and GIT_CONFIG_VALUE_<n>, which git reads from 2.31 on.
# An older git would ignore them and push anonymously, so refuse it.
if [ -n "${ASPEN_TOKEN_FILE:-}" ]; then
  git_version=$(git version)
  IFS=. read -r git_major git_minor _ <<<"${git_version#git version }"
  if ! [ "${git_major:-0}" -gt 2 ] 2>/dev/null &&
     ! { [ "${git_major:-0}" -eq 2 ] && [ "${git_minor:-0}" -ge 31 ]; } 2>/dev/null; then
    printf 'smoke.sh: ASPEN_TOKEN_FILE needs git 2.31 or later, found: %s\n' "$git_version" >&2
    exit 2
  fi
fi

# git with identity, protocol v2 and, when a token is given, an
# Authorization header scoped to ASPEN_URL only, so it never reaches a CDN
# or bucket a packfile URI points at (docs/PLAN.md, A7 review). The header
# goes in the environment, not in `-c`: a command line is visible to every
# user on the host (`ps`, /proc/<pid>/cmdline), the environment only to the
# same user and root. The variables are local to this function, so only
# the git calls that talk to Aspen get them.
git_aspen() {
  if [ -n "${ASPEN_TOKEN_FILE:-}" ]; then
    local basic n=${GIT_CONFIG_COUNT:-0}
    basic=$(printf 'smoke:%s' "$(tr -d '\n' <"$ASPEN_TOKEN_FILE")" | base64 | tr -d '\n')
    local -x GIT_CONFIG_COUNT=$((n + 1))
    local -x "GIT_CONFIG_KEY_$n=http.${ASPEN_URL}/.extraHeader"
    local -x "GIT_CONFIG_VALUE_$n=Authorization: Basic ${basic}"
  fi
  git -c user.name=smoke -c user.email=smoke@example.invalid -c protocol.version=2 "$@"
}

# --- Aspen -------------------------------------------------------------------

if [ "$(status_of "$ASPEN_URL/healthz")" = 200 ]; then pass "aspen /healthz"; else fail "aspen /healthz"; fi
if [ "$(status_of "$ASPEN_URL/readyz")" = 200 ]; then pass "aspen /readyz (database answers)"; else fail "aspen /readyz"; fi

if [ -n "${SMOKE_REPO:-}" ]; then
  repo_url="$ASPEN_URL/$SMOKE_REPO.git"
  if git_aspen clone -q "$repo_url" "$work/push" 2>"$work/clone.err"; then
    cd "$work/push"
    printf '%s\n' "$stamp" >"smoke-$stamp.txt"
    git add "smoke-$stamp.txt"
    git_aspen commit -qm "smoke $stamp"
    branch=$(git symbolic-ref --short HEAD)
    if git_aspen push -q origin "HEAD:refs/heads/$branch" 2>"$work/push.err"; then
      pass "push to $SMOKE_REPO ($branch)"
    else
      fail "push to $SMOKE_REPO: $(tail -1 "$work/push.err")"
    fi
    want=$(git rev-parse HEAD)
    cd - >/dev/null
    # A fresh clone that accepts packfile URIs, as an opted-in agent would.
    if GIT_TRACE_PACKET="$work/packet.log" git_aspen -c fetch.uriprotocols=https,http \
        clone -q "$repo_url" "$work/fetch" 2>"$work/fetch.err"; then
      got=$(git -C "$work/fetch" rev-parse HEAD)
      if [ "$got" = "$want" ]; then pass "clone returns the pushed commit"; else fail "clone returned $got, pushed $want"; fi
      if git -C "$work/fetch" fsck --strict --no-progress >/dev/null 2>&1; then pass "fsck --strict"; else fail "fsck --strict"; fi
      # The fetch response's packfile-uris section: "<pack hash> <url>".
      hosts=$(grep -oE '< [0-9a-f]{40} https?://[^/ ]+' "$work/packet.log" | awk '{print $3}' | sort -u | tr '\n' ' ' || true)
      if [ -n "$hosts" ]; then
        pass "packfile URIs served by ${hosts% }"
      else
        echo "info  no packfile URIs in this clone (the pack came inline)"
      fi
    else
      fail "clone of $SMOKE_REPO: $(tail -1 "$work/fetch.err")"
    fi
  else
    fail "clone of $SMOKE_REPO: $(tail -1 "$work/clone.err")"
  fi
fi

# --- CDN edge ----------------------------------------------------------------

if [ -n "${CDN_URL:-}" ]; then
  CDN_URL=${CDN_URL%/}
  cdn_root=${CDN_URL%/-/express}
  pack="repos/1/packs/0000000000000000000000000000000000000000.pack"
  code=$(status_of "$CDN_URL/k1/1/c2ln/$pack")
  if [ "$code" = 403 ]; then pass "CDN refuses an expired URL (403)"; else fail "CDN answered $code for an expired URL, want 403"; fi
  code=$(status_of "$cdn_root/")
  if [ "$code" = 404 ]; then pass "CDN serves nothing outside /-/express (404)"; else fail "CDN answered $code for /, want 404"; fi
  code=$(status_of "$CDN_URL/-/health")
  if [ "$code" = 200 ]; then pass "CDN health probe reaches express (200)"; else fail "CDN health probe answered $code"; fi
fi

# --- place -------------------------------------------------------------------

if [ -n "${PLACE_URL:-}" ]; then
  PLACE_URL=${PLACE_URL%/}
  if curl -fsS --max-time 20 "$PLACE_URL/" | grep -qi '<!doctype html>'; then pass "place page"; else fail "place page"; fi
  if config=$(curl -fsS --max-time 20 "$PLACE_URL/api/v1/config"); then
    pass "place API config"
  else
    fail "place API config"
    config='{}'
  fi
  width=$(printf '%s' "$config" | sed -n 's/.*"width":\([0-9]*\).*/\1/p')
  height=$(printf '%s' "$config" | sed -n 's/.*"height":\([0-9]*\).*/\1/p')
  x=$(( RANDOM % ${width:-64} ))
  y=$(( RANDOM % ${height:-64} ))
  if git clone -q "$PLACE_URL/canvas.git" "$work/canvas" 2>"$work/canvas.err"; then
    pass "clone canvas.git"
    mkdir -p "$work/canvas/tiles"
    printf '#d6262b\n' >"$work/canvas/tiles/${x}_${y}.txt"
    git -C "$work/canvas" add tiles
    git -C "$work/canvas" -c user.name=smoke -c user.email=smoke@example.invalid commit -qm "smoke $stamp"
    branch="smoke/$stamp"
    if git -C "$work/canvas" push -q origin "HEAD:refs/heads/$branch" 2>"$work/canvas-push.err"; then
      pass "push a tile to $branch"
      if curl -fsS --max-time 20 "$PLACE_URL/api/v1/canvas" | grep -q "\"$branch\""; then
        pass "the branch is on the canvas"
      else
        fail "the pushed branch is not on the canvas"
      fi
    else
      fail "push a tile: $(tail -1 "$work/canvas-push.err")"
    fi
  else
    fail "clone canvas.git: $(tail -1 "$work/canvas.err")"
  fi
fi

# --- SSH ---------------------------------------------------------------------

if [ -n "${SSH_HOST:-}" ] && [ -n "${SMOKE_REPO:-}" ]; then
  : "${SSH_KEY:?set SSH_KEY to a private key registered with aspen key add}"
  : "${SSH_KNOWN_HOSTS:?set SSH_KNOWN_HOSTS to a file with the pinned host key}"
  export GIT_SSH_COMMAND="ssh -i $SSH_KEY -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$SSH_KNOWN_HOSTS"
  if git ls-remote "ssh://git@$SSH_HOST/$SMOKE_REPO.git" >/dev/null 2>"$work/ssh.err"; then
    pass "ls-remote over SSH with the pinned host key"
  else
    fail "ls-remote over SSH: $(tail -1 "$work/ssh.err")"
  fi
fi

if [ "$failures" -gt 0 ]; then
  printf '%d check(s) failed\n' "$failures" >&2
  exit 1
fi
echo "all checks passed"
