#!/usr/bin/env bash
# Spike S4, duplicate-object hazard in thin-pack completion.
#
# gix-pack's LookupRefDeltaObjectsIter asks the external lookup for every
# REF_DELTA base it has not inserted itself. It never checks whether the
# incoming pack already contains that base. So when a pack carries object X
# and a REF_DELTA against X, and X is also in the push's deps, completion
# inserts a second copy of X.
#
# This script builds that situation with stock Git. pack-objects without
# --delta-base-offset writes every delta as REF_DELTA, which is what libgit2
# does for every delta (spike S1). Each case runs the harness's `thin` mode
# (lookup = the base repository's pack) with and without `--dedupe`, then
# checks the output .idx for duplicate object IDs and has stock Git verify it.
#
# usage: bench/spikes/s4/dup.sh <work-dir>
# Results are appended to results/dup.jsonl next to this file.
set -euo pipefail

work=$(realpath -m "$1")
here=$(cd "$(dirname "$0")" && pwd)
bin="$here/target/release/aspen-spike-s4"
out="$here/results/dup.jsonl"
(cd "$here" && cargo build --release --locked --quiet)
rm -rf "$work" && mkdir -p "$work" "$here/results"
: >"$out"
export GIT_AUTHOR_NAME=s4 GIT_AUTHOR_EMAIL=s4@example.invalid \
  GIT_COMMITTER_NAME=s4 GIT_COMMITTER_EMAIL=s4@example.invalid \
  GIT_AUTHOR_DATE='2026-11-03T00:00:00Z' GIT_COMMITTER_DATE='2026-11-03T00:00:00Z'

# A history of 30 commits to one growing file, so every blob after the first
# deltifies against a neighbour.
src="$work/src"
git init -q -b main "$src"
for i in $(seq 1 30); do
  seq 1 $((i * 200)) >"$src/f.txt"
  git -C "$src" add f.txt
  git -C "$src" commit -q -m "commit $i"
done

# "Trunk": a bare repository at commit 20, one pack. This is the push's deps.
base="$work/base.git"
git clone -q --bare --no-local "$src" "$base"
git -C "$base" update-ref refs/heads/main "$(git -C "$src" rev-parse HEAD~10)"
git -C "$base" reflog expire --expire=now --all
git -C "$base" repack -q -a -d -f
git -C "$base" prune
base_idx=$(ls "$base"/objects/pack/*.idx)

# Packs the client could send:
#   resend-ref:  the whole history, all REF_DELTA (libgit2 after a resend). Every
#                in-pack base is also in deps: the hazard.
#   thin-ref:    commits 21..30, thin, all REF_DELTA (an ordinary libgit2 push).
#                In-pack bases are new, thin bases are in deps: no hazard.
#   resend-ofs:  the whole history with OFS_DELTA (stock git): no REF_DELTA, no
#                hazard. Control.
git -C "$src" pack-objects -q --revs --stdout <<<"HEAD" >"$work/resend-ref.pack"
printf 'HEAD\n^HEAD~10\n' |
  git -C "$src" pack-objects -q --revs --thin --stdout >"$work/thin-ref.pack"
git -C "$src" pack-objects -q --revs --delta-base-offset --stdout <<<"HEAD" >"$work/resend-ofs.pack"

objects_in() { # pack -> object count from its header
  od -An -tu4 --endian=big -j8 -N4 "$1" | tr -d ' '
}

for p in resend-ref thin-ref resend-ofs; do
  for mode in plain dedupe; do
    o="$work/out-$p-$mode"
    flags=()
    [ "$mode" = dedupe ] && flags=(--dedupe)
    if stage=$("$bin" thin "$work/$p.pack" --base-idx "$base_idx" --out-dir "$o" "${flags[@]}" 2>"$o.err"); then
      idx=$(ls "$o"/*.idx)
      ids=$(git show-index <"$idx" | awk '{print $2}')
      total=$(wc -l <<<"$ids")
      unique=$(sort -u <<<"$ids" | wc -l)
      # Stock Git's own checks. Plain index-pack proves every delta resolves
      # inside the pack (self-contained). --strict also rejects an object
      # stored twice, but it checks connectivity too, so a completed thin push
      # (whose parents live in deps) fails it for that reason alone.
      cp "$o"/*.pack "$o/check.pack"
      git_ok=false strict_ok=false
      git index-pack -o "$o/check.idx" "$o/check.pack" >/dev/null 2>"$o/check.err" && git_ok=true
      rm -f "$o/check.idx"
      git index-pack --strict -o "$o/check.idx" "$o/check.pack" >/dev/null 2>"$o/strict.err" && strict_ok=true
      jq -cn --arg pack "$p" --arg mode "$mode" --argjson stage "$stage" \
        --argjson in "$(objects_in "$work/$p.pack")" \
        --argjson total "$total" --argjson unique "$unique" --argjson git_ok "$git_ok" --argjson strict_ok "$strict_ok" \
        --arg strict_err "$(head -c 300 "$o/strict.err")" \
        '{pack: $pack, mode: $mode, objects_in: $in, objects_out: $total, unique_ids_out: $unique,
          duplicates: ($total - $unique), git_index_pack_ok: $git_ok,
          git_index_pack_strict_ok: $strict_ok, strict_error: $strict_err, base_lookups: $stage.base_lookups, dedupe: $stage.dedupe}' |
        tee -a "$out"
    else
      jq -cn --arg pack "$p" --arg mode "$mode" --arg err "$(head -c 300 "$o.err")" \
        '{pack: $pack, mode: $mode, harness_error: $err}' | tee -a "$out"
    fi
  done
done
