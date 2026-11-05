#!/usr/bin/env bash
# Bring up a baseline Git server in Docker, run the aspen-bench workloads
# against it, and tear it down. See bench/README.md.
#
# Usage: bench/baselines/run.sh <target> [all|up|run|down|check]
#
#   all    up, run, then down (the default)
#   up     start the target and create the benchmark user
#   run    run every workload of the profile, one fresh repository each
#   down   stop the target and delete its data
#   check  compare the results in BENCH_OUT with the stored baselines in
#          bench/baselines/results/<BENCH_ENV>/<target>/; exits 1 on a
#          regression and 0 when there is no stored baseline yet
#
# Targets: git-http-backend, gitea, forgejo, gitlab, aspen (`aspen dev`
# built from this checkout; no Docker), aspen-no-compaction (the same with
# its background worker off), and `local` (the in-process git http-backend
# bridge; no Docker).
#
# Environment:
#   BENCH_PROFILE  smoke | nightly | full | storm1000 | real   (default: smoke)
#   BENCH_OUT      directory for JSON results (default: bench/results/<target>)
#   BENCH_PORT     host port for the target (each target has its own default)
#   BENCH_KEEP=1   with `all`, leave the target running afterwards
#   BENCH_ENV      name of the machine class for `check` (default: local)
#   BENCH_WORKLOADS  space-separated subset of the profile's workloads to run
#                  (writers fetch-storm fetch-storm-cold fetch-storm-depth1
#                  clone stale-branch; default: all of them)
#   BENCH_REPO_SOURCE  a bare repository whose main replaces the synthetic
#                  history in every workload (bench/README.md, "Real
#                  repositories")
#   BENCH_DEPTH1_JOBS, BENCH_DEPTH1_ROUNDS  override the profile's depth-1
#                  storm size (the campaign's threshold step)
set -euo pipefail

BASELINES="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$BASELINES/../.." && pwd)"
TARGET="${1:?usage: run.sh <target> [all|up|run|down|check]}"
ACTION="${2:-all}"
PROFILE="${BENCH_PROFILE:-smoke}"
OUT="${BENCH_OUT:-$REPO_ROOT/bench/results/$TARGET}"
STATE_FILE="$BASELINES/.state/$TARGET.env"

log() { echo "run.sh: $*" >&2; }

compose() {
  docker compose -f "$BASELINES/$TARGET/compose.yaml" "$@"
}

# wait_http <url> <timeout-seconds> [any]: poll until the URL answers 2xx,
# or any HTTP status at all when the third argument is `any`.
wait_http() {
  local url="$1" deadline=$((SECONDS + $2)) mode="${3:-ok}" code
  while true; do
    code="$(curl -sS -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || true)"
    if [[ "$code" == 2* ]] || [[ "$mode" == any && "$code" != 000 && -n "$code" ]]; then
      return 0
    fi
    if ((SECONDS >= deadline)); then
      log "timed out after $2s waiting for $url"
      return 1
    fi
    sleep 2
  done
}

# A random hex string for throwaway local credentials. (No `tr | head` on
# /dev/urandom: under `pipefail` the SIGPIPE that stops `tr` fails the script.)
random_secret() {
  od -An -N12 -tx1 /dev/urandom | tr -d ' \n'
}

# Throwaway credentials are kept in a mode-600 file outside version control
# so `up` and later `run` calls can share them.
save_state() {
  mkdir -p "$(dirname "$STATE_FILE")"
  touch "$STATE_FILE"
  chmod 600 "$STATE_FILE"
  printf '%s=%q\n' "$1" "$2" >>"$STATE_FILE"
}

load_state() {
  if [[ -f "$STATE_FILE" ]]; then
    # shellcheck disable=SC1090
    . "$STATE_FILE"
  fi
}

target_down() {
  compose down -v --remove-orphans
}

# The target's container logs, for diagnosing a failed run.
show_logs() {
  [[ "$TARGET" == local ]] && return 0
  if [[ "$TARGET" == aspen* ]]; then
    log "the run failed; last lines of the aspen log:"
    tail -n 80 "$ASPEN_DATA/aspen.log" >&2 || true
    return 0
  fi
  log "the run failed; last container logs:"
  compose logs --no-color --tail 80 >&2 || true
}

# Extra aspen-bench arguments for the workloads that push (writers and the
# fetch storms). A target.sh may set it; the aspen target needs
# --branch-scoped-push (see bench/README.md).
TARGET_PUSH_ARGS=()
# Extra arguments for the stale-branch workload; the aspen targets add
# --metrics-url so the planner's counters are recorded.
TARGET_STALE_ARGS=()

if [[ "$TARGET" == local ]]; then
  target_up() { :; }
  target_create_repo() { echo local; }
  target_down() { :; }
elif [[ -f "$BASELINES/$TARGET/target.sh" ]]; then
  # shellcheck disable=SC1090
  . "$BASELINES/$TARGET/target.sh"
else
  log "unknown target '$TARGET'"
  exit 2
fi

# Workload parameters per profile. Every run seeds a fresh repository with
# the synthetic history, so results don't depend on earlier runs.
case "$PROFILE" in
  smoke)
    REPO_SIZE=small
    WRITERS=(--writers 4 --pushes-per-writer 5)
    STORM=(--jobs 8 --rounds 2)
    STORM_COLD=(--jobs 4 --rounds 2)
    STORM_DEPTH1=(--jobs 8 --rounds 2)
    CLONE=(--concurrency 2 --clones 4)
    CLONE_REPO_SIZE=small
    STALE=(--points 10,100 --repeats 2)
    ;;
  nightly)
    REPO_SIZE=small
    WRITERS=(--writers 16 --duration-secs 30)
    STORM=(--jobs 64 --rounds 3)
    STORM_COLD=(--jobs 16 --rounds 2)
    STORM_DEPTH1=(--jobs 64 --rounds 2)
    CLONE=(--concurrency 4 --clones 12)
    CLONE_REPO_SIZE=medium
    STALE=(--points 10,100,1000 --repeats 3)
    ;;
  full)
    # Section 4.6 scale. Needs a large load-generator machine (thousands of
    # git processes; raise `ulimit -n` and `ulimit -u`), ideally separate
    # from the server.
    REPO_SIZE=medium
    WRITERS=(--writers 1000 --duration-secs 120)
    STORM=(--jobs 1000 --rounds 3)
    STORM_COLD=(--jobs 100 --rounds 2)
    STORM_DEPTH1=(--jobs 1000 --rounds 3)
    CLONE=(--concurrency 16 --clones 64)
    CLONE_REPO_SIZE=large
    STALE=(--points 10,100,1000,10000 --repeats 5)
    ;;
  storm1000)
    # D2's section-4.6 storms on the small synthetic repository: 1,000
    # concurrent jobs per round (bench/campaign/README.md).
    REPO_SIZE=small
    WRITERS=(--writers 16 --duration-secs 30)
    STORM=(--jobs 1000 --rounds 3)
    STORM_COLD=(--jobs 100 --rounds 2)
    STORM_DEPTH1=(--jobs 1000 --rounds 3)
    CLONE=(--concurrency 4 --clones 12)
    CLONE_REPO_SIZE=small
    STALE=(--points 10,100,1000 --repeats 3)
    ;;
  real)
    # D2's real-repository profile, for BENCH_REPO_SOURCE (a pinned clone
    # of a real project; bench/campaign/README.md). Sizes are per round:
    # a cold job or a clone is a full copy of that history.
    REPO_SIZE=small
    WRITERS=(--writers 16 --duration-secs 30)
    STORM=(--jobs 1000 --rounds 2)
    STORM_COLD=(--jobs 8 --rounds 2)
    STORM_DEPTH1=(--jobs 1000 --rounds 2)
    CLONE=(--concurrency 4 --clones 8)
    CLONE_REPO_SIZE=small
    STALE=(--points 10,100,1000 --repeats 3)
    ;;
  *)
    log "unknown BENCH_PROFILE '$PROFILE'"
    exit 2
    ;;
esac

if [[ -n "${BENCH_DEPTH1_JOBS:-}" ]]; then
  STORM_DEPTH1=(--jobs "$BENCH_DEPTH1_JOBS" --rounds "${BENCH_DEPTH1_ROUNDS:-1}")
fi

BENCH_BIN="$REPO_ROOT/target/release/aspen-bench"
# Recorded in every result (environment.aspen_commit); CI sets GITHUB_SHA.
if [[ -z "${ASPEN_BENCH_COMMIT:-}" && -z "${GITHUB_SHA:-}" ]]; then
  ASPEN_BENCH_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)"
  export ASPEN_BENCH_COMMIT
fi

run_workloads() {
  log "building aspen-bench"
  (cd "$REPO_ROOT" && cargo build --release --locked -p aspen-bench)
  mkdir -p "$OUT"
  local stamp url
  stamp="$(date +%s)"
  # name, subcommand, repo size, extra args...
  local source=()
  if [[ -n "${BENCH_REPO_SOURCE:-}" ]]; then
    source=(--repo-source "$BENCH_REPO_SOURCE")
  fi
  run_one() {
    local name="$1" sub="$2" size="$3"
    shift 3
    if [[ -n "${BENCH_WORKLOADS:-}" && " $BENCH_WORKLOADS " != *" $name "* ]]; then
      return 0
    fi
    url="$(target_create_repo "bench-$name-$stamp")"
    log "$TARGET: $name ($PROFILE)"
    "$BENCH_BIN" "$sub" --url "$url" --label "$TARGET" --repo-size "$size" \
      ${source[@]+"${source[@]}"} --out "$OUT/$name.json" "$@"
  }
  # The ${a[@]+...} form keeps an empty array safe under `set -u` in bash 3.2.
  run_one writers writers "$REPO_SIZE" "${WRITERS[@]}" ${TARGET_PUSH_ARGS[@]+"${TARGET_PUSH_ARGS[@]}"}
  run_one fetch-storm fetch-storm "$REPO_SIZE" "${STORM[@]}" --mode warm ${TARGET_PUSH_ARGS[@]+"${TARGET_PUSH_ARGS[@]}"}
  run_one fetch-storm-cold fetch-storm "$REPO_SIZE" "${STORM_COLD[@]}" --mode cold ${TARGET_PUSH_ARGS[@]+"${TARGET_PUSH_ARGS[@]}"}
  run_one fetch-storm-depth1 fetch-storm "$REPO_SIZE" "${STORM_DEPTH1[@]}" --mode cold --depth 1 ${TARGET_PUSH_ARGS[@]+"${TARGET_PUSH_ARGS[@]}"}
  run_one clone clone "$CLONE_REPO_SIZE" "${CLONE[@]}"
  run_one stale-branch stale-branch "$REPO_SIZE" "${STALE[@]}" ${TARGET_PUSH_ARGS[@]+"${TARGET_PUSH_ARGS[@]}"} ${TARGET_STALE_ARGS[@]+"${TARGET_STALE_ARGS[@]}"}
  log "results in $OUT"
}

check_results() {
  local baseline_dir="$BASELINES/results/${BENCH_ENV:-local}/$TARGET"
  (cd "$REPO_ROOT" && cargo build --release --locked -p aspen-bench)
  if ! compgen -G "$baseline_dir/*.json" >/dev/null; then
    log "no stored baselines in $baseline_dir; nothing to compare"
    return 0
  fi
  local failed=0 baseline name
  for baseline in "$baseline_dir"/*.json; do
    name="$(basename "$baseline")"
    if [[ ! -f "$OUT/$name" ]]; then
      log "missing result $OUT/$name for stored baseline $baseline"
      failed=1
      continue
    fi
    "$BENCH_BIN" compare --baseline "$baseline" --current "$OUT/$name" \
      --tolerances "$REPO_ROOT/bench/tolerances.json" || failed=1
  done
  return "$failed"
}

case "$ACTION" in
  check) check_results ;;
  up)
    rm -f "$STATE_FILE"
    target_up
    ;;
  run) run_workloads ;;
  down)
    target_down
    rm -f "$STATE_FILE"
    ;;
  all)
    rm -f "$STATE_FILE"
    if [[ "${BENCH_KEEP:-0}" != 1 ]]; then
      trap 'status=$?; ((status == 0)) || show_logs; target_down; rm -f "$STATE_FILE"' EXIT
    fi
    target_up
    run_workloads
    ;;
  *)
    log "unknown action '$ACTION'; use all, up, run, down or check"
    exit 2
    ;;
esac
