#!/usr/bin/env bash
# Reproduce spike S4: ingest one pack with gix-pack and with stock Git, and
# record throughput and memory as JSON lines.
#
# usage: bench/spikes/s4/run.sh <file.pack> <work-dir> [threads...]
#
#   <file.pack>  any pack with its .idx beside it, e.g. from
#                `git clone --bare https://github.com/torvalds/linux.git`
#   <work-dir>   scratch space on a real disk (not tmpfs), with room for two
#                more copies of the pack
#   threads      thread counts to try for indexing (default: 1 and nproc)
#
# STAGES (default "stream git gix commits"; also "thinpath") picks what to run, so a huge pack
# can be measured in several shorter invocations. `commits` needs the gix
# output, which `gix` leaves in <work-dir>/gix.
#
# Results are appended to results/<repo-name>.jsonl next to this file.
set -euo pipefail

pack=$(realpath "$1")
work=$(realpath -m "$2")
shift 2
threads=("$@")
[ ${#threads[@]} -gt 0 ] || threads=(1 "$(nproc)")

here=$(cd "$(dirname "$0")" && pwd)
idx=${pack%.pack}.idx
name=$(basename "$(dirname "$(dirname "$(dirname "$pack")")")" .git)
out="$here/results/$name.jsonl"
measure="$here/measure.py"
bin="$here/target/release/aspen-spike-s4"

if findmnt -n -o FSTYPE --target "$work" 2>/dev/null | grep -q tmpfs; then
  echo "error: $work is on tmpfs (RAM); use a real disk" >&2
  exit 1
fi
mkdir -p "$work" "$here/results"
export TMPDIR="$work"
(cd "$here" && cargo build --release --locked --quiet)

record() { # label command...
  local label=$1
  shift
  local stats
  : >"$work/stage.json"
  stats=$("$measure" "$label" "$*" 2>&1 >"$work/stage.json" | tail -1)
  # The harness prints its own JSON; for `git index-pack` there is none.
  [ -s "$work/stage.json" ] || echo '{}' >"$work/stage.json"
  jq -c --arg run "$label" --argjson m "$stats" --arg host "$(hostname -s)" \
    --arg cpus "$(nproc)" --arg git "$(git --version)" --arg pack "$(basename "$pack")" \
    '{run: $run, pack: $pack} + . + {measured: $m, host: $host, cpus: ($cpus|tonumber), git: $git}' \
    "$work/stage.json" >>"$out"
  echo "$stats"
}

echo "pack: $pack ($(du -h "$pack" | cut -f1)); results: $out"
# Warm the page cache so every run reads the input from memory: this measures
# ingest CPU and memory, not the scratch disk.
cat "$pack" >/dev/null

has() { [[ " ${STAGES:-stream git gix commits} " == *" $1 "* ]]; }

if has stream; then
  # Pass 1 only: streaming parse + trailer check, with and without the spool copy.
  record stream "cat '$pack' | '$bin' stream -"
  record stream+spool "cat '$pack' | '$bin' stream - --spool '$work/spool.pack'"
  cmp "$pack" "$work/spool.pack" # the spooled bytes are exactly the input
  rm -f "$work/spool.pack"
fi

for t in "${threads[@]}"; do
  if has git; then
    # Stock Git: what receive-pack runs (`index-pack --stdin`), into an empty repo.
    rm -rf "$work/git.git" && git init -q --bare "$work/git.git"
    record "git-index-pack-t$t" \
      "cat '$pack' | git -C '$work/git.git' index-pack --stdin --threads=$t >/dev/null"
    rm -rf "$work/git.git"
  fi
  if has gix; then
    rm -rf "$work/gix" && mkdir "$work/gix"
    record "gix-index-t$t" "cat '$pack' | '$bin' index - --out-dir '$work/gix' --threads $t"
    cmp "$idx" "$work/gix"/*.idx # the gix .idx must be byte-identical to Git's
  fi
  if has thinpath; then
    # The same pack through the thin-completion code path (base lookup on,
    # never hit here), to price "every push takes the completion path".
    rm -rf "$work/thinpath" && mkdir "$work/thinpath"
    record "gix-thin-path-t$t" \
      "cat '$pack' | '$bin' thin - --base-idx '$idx' --out-dir '$work/thinpath' --threads $t"
    cmp "$pack" "$work/thinpath"/*.pack # no base inserted: output is the input
    rm -rf "$work/thinpath"
  fi
done

if has commits; then
  # Second traversal to extract commits, checked against `git log`.
  record commits "'$bin' commits --pack '$work/gix'/*.pack --idx '$work/gix'/*.idx --out '$work/commits.tsv'"
  git --git-dir "$(dirname "$(dirname "$(dirname "$pack")")")" log --all --format='%H %T %P' |
    sed 's/ *$//' | LC_ALL=C sort >"$work/git-commits.tsv"
  LC_ALL=C sort -o "$work/commits.tsv" "$work/commits.tsv"
  cmp "$work/commits.tsv" "$work/git-commits.tsv"
  echo "commit graph matches git log ($(wc -l <"$work/commits.tsv") commits)"
  rm -rf "$work/gix" "$work"/*.tsv
fi
rm -f "$work/stage.json"
