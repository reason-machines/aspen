#!/usr/bin/env bash
# Runs Aspen's fuzz targets under libFuzzer (fuzz/README.md).
#
#   fuzz/run.sh [-t SECONDS] [-w WORKDIR] [-j JOBS] [TARGET...]
#
# Each target runs for SECONDS (default 60) on a working corpus in
# WORKDIR/corpus/<target> (default $TMPDIR/aspen-fuzz), seeded from the
# committed fuzz/corpus/<target> and fuzz/regressions/<target> and kept
# between runs, with JOBS parallel fuzzing processes (default 1). Crashes,
# timeouts and out-of-memory inputs go to WORKDIR/artifacts/<target>; the
# script exits non-zero if any target found one. With no TARGET, every
# target runs, one after another.
#
# Needs a nightly toolchain and cargo-fuzz. Set FUZZ_TOOLCHAIN to pick the
# nightly (default: the one CI pins below).
set -euo pipefail

FUZZ_TOOLCHAIN="${FUZZ_TOOLCHAIN:-nightly-2026-11-02}"
seconds=60
work="${TMPDIR:-/tmp}/aspen-fuzz"
jobs=1
while getopts "t:w:j:" opt; do
  case "$opt" in
    t) seconds="$OPTARG" ;;
    w) work="$OPTARG" ;;
    j) jobs="$OPTARG" ;;
    *) sed -n '2,15p' "$0"; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
case "$seconds$jobs" in
  *[!0-9]*) echo "-t and -j take whole numbers" >&2; exit 2 ;;
esac

repo="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo"
fuzz_dir=fuzz/libfuzzer
if [ "$#" -eq 0 ]; then
  mapfile -t targets < <(sed -n 's/^name = "\(.*\)"$/\1/p' "$fuzz_dir/Cargo.toml" | grep -v '^aspen-fuzz-libfuzzer$')
else
  targets=("$@")
fi
for t in "${targets[@]}"; do
  if [ ! -f "$fuzz_dir/fuzz_targets/$t.rs" ]; then
    echo "unknown target $t; see $fuzz_dir/fuzz_targets/" >&2
    exit 2
  fi
done

cargo "+$FUZZ_TOOLCHAIN" fuzz build --fuzz-dir "$fuzz_dir" -O

# Each target's limits: inputs are small, and the targets themselves cap
# objects at 1 MiB and packs at 16 MiB inflated, so 2 GiB of RSS or a
# single 1 GiB allocation means a size check is missing.
# Fork mode ignores timeouts and out-of-memory inputs unless told not to,
# and those are exactly what a missing size check produces.
fork=()
if [ "$jobs" -gt 1 ]; then
  fork=(-fork="$jobs" -ignore_crashes=0 -ignore_timeouts=0 -ignore_ooms=0)
fi
failed=()
for t in "${targets[@]}"; do
  corpus="$work/corpus/$t"
  artifacts="$work/artifacts/$t"
  mkdir -p "$corpus" "$artifacts"
  cp -n fuzz/corpus/"$t"/* "$corpus"/ 2>/dev/null || true
  if [ -d "fuzz/regressions/$t" ]; then
    cp -n fuzz/regressions/"$t"/* "$corpus"/ 2>/dev/null || true
  fi
  echo "::group::$t (${seconds}s, $jobs jobs)"
  set +e
  cargo "+$FUZZ_TOOLCHAIN" fuzz run --fuzz-dir "$fuzz_dir" -O "$t" "$corpus" -- \
    -max_total_time="$seconds" \
    -timeout=30 \
    -rss_limit_mb=2048 \
    -malloc_limit_mb=1024 \
    -max_len=1048576 \
    "${fork[@]}" \
    -artifact_prefix="$artifacts/" \
    -print_final_stats=1
  status=$?
  set -e
  echo "::endgroup::"
  if [ "$status" -ne 0 ] || [ -n "$(ls -A "$artifacts")" ]; then
    echo "$t: libFuzzer found a failing input (exit $status); see $artifacts" >&2
    failed+=("$t")
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "failing targets: ${failed[*]}" >&2
  echo "reproduce on stable: cargo run -p aspen-fuzz -- run <target> <artifact>" >&2
  exit 1
fi
echo "all targets ran clean: ${targets[*]}"
