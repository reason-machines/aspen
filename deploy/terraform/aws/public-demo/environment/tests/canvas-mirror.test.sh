#!/bin/sh
# Runs canvas-mirror.sh (the place task's mirror sidecar) against local
# repositories:
#
#   sh deploy/terraform/aws/public-demo/environment/tests/canvas-mirror.test.sh
#
# Place is a bare repository that appears only some seconds after the
# sidecar starts, the way aspen-place listens only once it has loaded the
# canvas; GitHub is a bare repository that a `git` wrapper puts in place of
# https://github.com/<repo>.git (and `apk` does nothing). Checks that:
#
# 1. with a 15-minute MIRROR_INTERVAL, main is on "GitHub" within seconds of
#    place answering, not an interval later (the sidecar used to fetch once
#    at start, fail, and wait the whole interval);
# 2. when place never answers, the sidecar stops waiting after one interval
#    and carries on with its usual runs;
# 3. a MIRROR_START_RETRY that is not a positive number of seconds is
#    reported and replaced by 5, and the wait still ends once place answers;
# 4. a place that accepts the request and never answers (here a `git` that
#    sleeps) cannot hold the wait: each ask is cut off after 10 seconds.
#
# Needs git, pkill and timeout.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
script="$here/../canvas-mirror.sh"
real_git=$(command -v git)

dir=$(mktemp -d)
pids=""
cleanup() {
  # The sidecar's children first (its sleep), then the sidecar.
  for pid in $pids; do
    pkill -P "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
  done
  rm -rf "$dir"
}
trap cleanup EXIT

mkdir -p "$dir/bin"
printf '#!/bin/sh\nexit 0\n' > "$dir/bin/apk"
cat > "$dir/bin/git" <<EOF
#!/bin/sh
# Every argument naming the GitHub repository names the local stand-in.
# A git naming the stalled place hangs, as a place that took the request
# and never answered would.
for a in "\$@"; do
  shift
  if [ "\$a" = "https://github.com/example/place-canvas.git" ]; then a="\$GITHUB_STANDIN"; fi
  if [ -n "\${STALLED_PLACE:-}" ] && [ "\$a" = "\$STALLED_PLACE" ]; then exec sleep 600; fi
  set -- "\$@" "\$a"
done
exec "$real_git" "\$@"
EOF
chmod +x "$dir/bin/apk" "$dir/bin/git"

fail() {
  echo "FAIL: $*" >&2
  for log in "$dir"/*.log; do
    [ -f "$log" ] || continue
    echo "--- $log" >&2
    cat "$log" >&2
  done
  exit 1
}

# start_sidecar <name> <interval> <place> [<start retry>] [<stalled place>]:
# starts the sidecar (start retry 1 s by default), logging to
# $dir/<name>.log, with GitHub at $dir/<name>-github.git (empty).
start_sidecar() {
  "$real_git" init --quiet --bare "$dir/$1-github.git"
  mkdir -p "$dir/$1-home" "$dir/$1-tmp"
  env -u GIT_CONFIG_GLOBAL -u GIT_CONFIG_SYSTEM \
    PATH="$dir/bin:$PATH" HOME="$dir/$1-home" TMPDIR="$dir/$1-tmp" GIT_CONFIG_NOSYSTEM=1 \
    GITHUB_STANDIN="$dir/$1-github.git" \
    MIRROR_REPO=example/place-canvas MIRROR_CREDENTIAL=test-token \
    MIRROR_SOURCE="$3" MIRROR_INTERVAL="$2" MIRROR_START_RETRY="${4-1}" STALLED_PLACE="${5:-}" \
    sh "$script" > "$dir/$1.log" 2>&1 &
  pids="$pids $!"
}

# wait_for <seconds> <command...>: true once the command succeeds.
wait_for() {
  n=$1
  shift
  while [ "$n" -gt 0 ]; do
    if "$@"; then return 0; fi
    sleep 1
    n=$((n - 1))
  done
  "$@"
}

# A place repository with one commit on main, appearing at $1 all at once.
make_place() {
  "$real_git" init --quiet --bare "$1.new"
  work="$dir/work-$(basename "$1")"
  "$real_git" init --quiet "$work"
  (
    cd "$work"
    printf 'canvas\n' > tile
    "$real_git" add tile
    "$real_git" -c user.name=place -c user.email=place@example.com commit --quiet -m canvas
    "$real_git" push --quiet "$1.new" HEAD:refs/heads/main
  )
  mv "$1.new" "$1"
}

github_main() { "$real_git" --git-dir="$1" rev-parse --verify --quiet refs/heads/main || true; }

# 1. Place comes up 3 seconds after the sidecar; the interval is 15 minutes.
start_sidecar late 900 "$dir/late-place.git"
sleep 3
make_place "$dir/late-place.git"
tip=$("$real_git" --git-dir="$dir/late-place.git" rev-parse refs/heads/main)
started=$(date +%s)
wait_for 20 grep -qs "canvas-mirror: pushed main $tip" "$dir/late.log" ||
  fail "main was not pushed within 20 seconds of place answering (interval 900 s)"
took=$(($(date +%s) - started))
[ "$(github_main "$dir/late-github.git")" = "$tip" ] || fail "GitHub's main is not place's main"
grep -qs "canvas-mirror: waiting for place" "$dir/late.log" || fail "the wait was not logged"
if grep -qs "fetch from .* failed" "$dir/late.log"; then fail "a fetch failed while place was starting"; fi
echo "ok: main mirrored ${took}s after place answered (interval 900 s)"

# 2. Place never answers; the interval is 3 seconds.
start_sidecar never 3 "$dir/never-place.git"
wait_for 20 grep -qs "fetch from $dir/never-place.git failed" "$dir/never.log" ||
  fail "the sidecar did not carry on after waiting one interval"
grep -Eqs "canvas-mirror: place still does not answer at $dir/never-place.git after [0-9]+s" "$dir/never.log" ||
  fail "giving up the wait was not logged"
echo "ok: the wait ends after one interval and the usual runs go on"

# 3. A start retry of 0 (which used to spin without counting time) is
#    replaced by 5; place comes up 3 seconds after the sidecar.
start_sidecar zero 900 "$dir/zero-place.git" 0
sleep 3
make_place "$dir/zero-place.git"
tip=$("$real_git" --git-dir="$dir/zero-place.git" rev-parse refs/heads/main)
wait_for 20 grep -qs "canvas-mirror: pushed main $tip" "$dir/zero.log" ||
  fail "with MIRROR_START_RETRY=0, main was not pushed within 20 seconds of place answering"
grep -qs "canvas-mirror: MIRROR_START_RETRY=0 is not a positive number of seconds; using 5" "$dir/zero.log" ||
  fail "MIRROR_START_RETRY=0 was not reported"
echo "ok: MIRROR_START_RETRY=0 falls back to 5 s and main is mirrored"

# 4. Place takes the request and never answers; the interval is 3 seconds.
start_sidecar stalled 3 "$dir/stalled-place.git" 1 "$dir/stalled-place.git"
wait_for 25 grep -Eqs "canvas-mirror: place still does not answer at $dir/stalled-place.git after [0-9]+s" "$dir/stalled.log" ||
  fail "a stalled place held the wait past its interval"
echo "ok: a stalled place cannot hold the wait"
