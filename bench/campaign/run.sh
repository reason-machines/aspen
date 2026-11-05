#!/usr/bin/env bash
# The D2 benchmark campaign: every run behind bench/campaign/README.md, in
# order, into one directory that `aspen-bench report` turns into tables.
#
# Usage: bench/campaign/run.sh <out-dir> [step...]
#
# Steps (default: all of them, in this order):
#   stale       stale-branch fetch, N up to 10,000 (full profile): aspen,
#               aspen-no-compaction, local                    (~45 min)
#   synthetic   writers, fetch storms and clones, nightly profile, small
#               synthetic repository: aspen, local            (~5 min)
#   storm1000   1,000-job warm and depth-1 fetch storms, small synthetic
#               repository: aspen, local                      (~10 min)
#   real        every workload but stale-branch on a real repository, git.git
#               at v2.47.0 (fetched once into <out-dir>/.source): aspen,
#               local                                          (~45 min)
#   threshold   the git.git depth-1 storm at 250 and 500 jobs, one round
#               each, a fresh aspen server per size: aspen      (~5 min)
#   idx         the >2 GiB .idx comparison with git index-pack (~3 min)
#
# Needs what bench/README.md lists (git, a Rust toolchain), about 15 GB of
# disk, and for `real` network access to github.com. Results go to
# <out-dir>/<group>/<target>/<workload>.json; the report is
# <out-dir>/REPORT.md.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${1:?usage: bench/campaign/run.sh <out-dir> [step...]}"
shift
STEPS=("$@")
if ((${#STEPS[@]} == 0)); then
  STEPS=(stale synthetic storm1000 real threshold idx)
fi
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
RUN="$ROOT/bench/baselines/run.sh"
REAL_URL=https://github.com/git/git
REAL_TAG=v2.47.0

log() { echo "campaign: $*" >&2; }

# Recorded in every result as environment.aspen_commit.
commit="$(git -C "$ROOT" rev-parse HEAD)"
if [[ -n "$(git -C "$ROOT" status --porcelain --untracked-files=no)" ]]; then
  commit="$commit-dirty"
fi
export ASPEN_BENCH_COMMIT="$commit"

# run <group> <profile> <workloads> <targets...>
run() {
  local group="$1" profile="$2" workloads="$3"
  shift 3
  local t
  for t in "$@"; do
    log "$group: $t ($profile: $workloads)"
    BENCH_PROFILE="$profile" BENCH_WORKLOADS="$workloads" \
      BENCH_OUT="$OUT/$group/$t" "$RUN" "$t" all
  done
}

real_source() {
  local src="$OUT/.source/git.git"
  if [[ ! -d "$src" ]]; then
    log "fetching $REAL_URL at $REAL_TAG"
    mkdir -p "$OUT/.source"
    git clone --quiet --bare --no-tags --single-branch -b "$REAL_TAG" "$REAL_URL" "$src"
  fi
  git -C "$src" update-ref refs/heads/main "$(git -C "$src" rev-parse "$REAL_TAG^{commit}")"
  echo "$src"
}

for step in "${STEPS[@]}"; do
  case "$step" in
    stale) run synthetic full stale-branch aspen aspen-no-compaction local ;;
    synthetic)
      run synthetic nightly "writers fetch-storm fetch-storm-cold fetch-storm-depth1 clone" aspen local
      ;;
    storm1000) run storm1000 storm1000 "fetch-storm fetch-storm-depth1" aspen local ;;
    real)
      BENCH_REPO_SOURCE="$(real_source)" \
        run real-git real "writers fetch-storm fetch-storm-cold fetch-storm-depth1 clone" aspen local
      ;;
    threshold)
      src="$(real_source)"
      for jobs in 250 500; do
        log "threshold: aspen, depth-1 storm of $jobs jobs"
        BENCH_PROFILE=real BENCH_WORKLOADS=fetch-storm-depth1 BENCH_REPO_SOURCE="$src" \
          BENCH_DEPTH1_JOBS="$jobs" BENCH_DEPTH1_ROUNDS=1 \
          BENCH_OUT="$OUT/real-git-threshold/aspen-$jobs-jobs" "$RUN" aspen all
      done
      ;;
    idx)
      log "idx: the >2 GiB .idx comparison"
      (cd "$ROOT" && cargo test --release --locked -p aspen-pack --lib -- \
        --ignored over_2_gib --nocapture) 2>&1 | tee "$OUT/idx-over-2-gib.log"
      ;;
    *)
      log "unknown step '$step'"
      exit 2
      ;;
  esac
done

(cd "$ROOT" && cargo build --release --locked -p aspen-bench)
"$ROOT/target/release/aspen-bench" report "$OUT" >"$OUT/REPORT.md"
log "report in $OUT/REPORT.md"
