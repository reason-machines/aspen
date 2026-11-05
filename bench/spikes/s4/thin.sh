#!/usr/bin/env bash
# Spike S4, thin-pack completion (push step 2 in docs/PLAN.md): complete a
# thin pack against a base repository with gix-pack and with stock Git, prove
# the result is self-contained, and time the hash-and-upload pass alone.
#
# usage: bench/spikes/s4/thin.sh <base.git> <thin.pack> <work-dir> [threads]
#
#   <base.git>   bare repository holding the bases ("trunk"), one pack
#   <thin.pack>  e.g. printf 'HEAD\n^HEAD~100\n' |
#                  git --git-dir=<base.git> pack-objects --revs --thin \
#                    --delta-base-offset --stdout > push-100.pack
#   <work-dir>   scratch space on a real disk with room for 3 copies of the pack
#
# Results are appended to results/thin.jsonl next to this file.
set -euo pipefail

base=$(realpath "$1")
thin=$(realpath "$2")
work=$(realpath -m "$3")
t=${4:-$(nproc)}

here=$(cd "$(dirname "$0")" && pwd)
out="$here/results/thin.jsonl"
measure="$here/measure.py"
bin="$here/target/release/aspen-spike-s4"
base_idx=$(ls "$base"/objects/pack/*.idx)

if findmnt -n -o FSTYPE --target "$work" 2>/dev/null | grep -q tmpfs; then
  echo "error: $work is on tmpfs (RAM); use a real disk" >&2
  exit 1
fi
mkdir -p "$work" "$here/results"
export TMPDIR="$work"
(cd "$here" && cargo build --release --locked --quiet)

record() { # run-label command...
  local label=$1
  shift
  local stats
  : >"$work/stage.json"
  stats=$("$measure" "$label" "$*" 2>&1 >"$work/stage.json" | tail -1)
  [ -s "$work/stage.json" ] || echo '{}' >"$work/stage.json"
  jq -c --arg run "$label" --argjson m "$stats" --arg host "$(hostname -s)" \
    --arg cpus "$(nproc)" --arg git "$(git --version)" --arg pack "$(basename "$thin")" \
    --argjson size "$(stat -c %s "$thin")" \
    '{run: $run, pack: $pack, pack_bytes_in: $size} + . + {measured: $m, host: $host, cpus: ($cpus|tonumber), git: $git}' \
    "$work/stage.json" >>"$out"
  echo "$stats"
}

echo "thin pack: $thin ($(du -h "$thin" | cut -f1)); base: $base; results: $out"
cat "$thin" "$base_idx" >/dev/null # warm the page cache

# Stock Git, as receive-pack runs it: the target repository borrows the base
# repository's objects through alternates.
rm -rf "$work/git.git" && git init -q --bare "$work/git.git"
echo "$base/objects" >"$work/git.git/objects/info/alternates"
record "git-fix-thin-t$t" \
  "cat '$thin' | git -C '$work/git.git' index-pack --stdin --fix-thin --threads=$t >/dev/null"
git_out=$(stat -c %s "$work/git.git"/objects/pack/*.pack)
jq -c --argjson b "$git_out" '. + {pack_bytes_out: $b}' <<<"$(tail -1 "$out")" >"$work/last.json"
sed -i '$ d' "$out" && cat "$work/last.json" >>"$out" && rm "$work/last.json"
rm -rf "$work/git.git"

# gix: complete while streaming, then re-hash, then resolve and index.
rm -rf "$work/gix" && mkdir "$work/gix"
record "gix-thin-t$t" \
  "cat '$thin' | '$bin' thin - --base-idx '$base_idx' --out-dir '$work/gix' --threads $t"
completed=$(ls "$work/gix"/*.pack)

# Self-contained: stock Git indexes it with no alternates and no --fix-thin,
# and its .idx is byte-identical to the one gix wrote.
rm -rf "$work/check.git" && git init -q --bare "$work/check.git"
git -C "$work/check.git" index-pack --stdin --threads="$t" <"$completed" >/dev/null
cmp "$(ls "$work/check.git"/objects/pack/*.idx)" "${completed%.pack}.idx"
echo "completed pack is self-contained; stock git agrees on the .idx"
rm -rf "$work/check.git"

# Pass 2 on its own: one sequential SHA-1 read of the completed pack, with and
# without copying it (the copy stands in for the multipart upload).
cat "$completed" >/dev/null
record rehash "'$bin' rehash '$completed'"
record rehash+copy "'$bin' rehash '$completed' --copy-to '$work/upload.pack'"
rm -rf "$work/gix" "$work/upload.pack" "$work/stage.json"
