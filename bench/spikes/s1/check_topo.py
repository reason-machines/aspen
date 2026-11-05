#!/usr/bin/env python3
"""Randomised check of the recommended stitcher (packlib.stitch_topo).

Builds random stitch plans from the scenario packs (1-5 segments, repeats and
any order allowed, so duplicates, forward references and thin bases all show
up) and checks every output against:

  * stock git `index-pack --strict --fix-thin` for each git in GIT_VERSIONS,
    in a repository that already holds C1..C5 (pack AB)
    (--strict makes index-pack die on duplicate object ids; git <= 2.49 also
    dies when a REF_DELTA base appears twice);
  * structural invariants that go-git and old git need:
      - no object id appears twice,
      - every REF_DELTA base is either earlier in the pack or (thin mode only)
        outside it,
      - no delta is resolved through an external copy of an object the pack
        also contains (shadowed_thin_bases == 0, i.e. no hidden delta cycle);
  * when there is nothing to fix (no duplicates, no forward references, no
    bases to add) the output is byte-identical to the plain stitcher;
  * the output holds exactly the union of the segments' objects (plus, in
    non-thin mode, the external bases).

It then checks the single-pass property (see check_single_pass).

Usage: check_topo.py [--rounds N] [--seed S]   (needs ./setup.sh git first)
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import make_scenarios as ms  # noqa: E402
import packlib  # noqa: E402
import run_matrix as rm  # noqa: E402

SEGMENTS = ["A", "B", "C", "AB", "FULL", "Bsc", "Csc"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=300)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--work", default=os.path.join(tempfile.gettempdir(), "aspen-s1-topo"))
    a = ap.parse_args()
    work = os.path.abspath(a.work)
    subprocess.run([sys.executable, os.path.join(HERE, "make_scenarios.py"), "--git", rm.REF_GIT, "--out", work],
                   check=True, stdout=subprocess.DEVNULL)
    g = ms.Git(rm.REF_GIT, os.path.join(work, "origin"))
    scratch = tempfile.mkdtemp(dir=work)
    packs = {n: open(os.path.join(work, "packs", f"{n}.pack"), "rb").read() for n in SEGMENTS}
    maps = {n: ms.oid_map(g, os.path.join(work, "packs", f"{n}.pack"), n in ("B", "C"), scratch) for n in SEGMENTS}
    origin = {}
    for line in g("cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)").decode().splitlines():
        h, t = line.split()
        origin[bytes.fromhex(h)] = ({"commit": 1, "tree": 2, "blob": 3, "tag": 4}[t], g("cat-file", t, h))
    gits = [os.path.join(rm.CACHE, f"git-{v}", "bin", "git") for v in rm.GIT_VERSIONS]
    env = rm.git_env(work)
    rng = random.Random(a.seed)
    stats = {"rounds": 0, "with_duplicates": 0, "with_forward_refs": 0, "thin": 0, "nonthin": 0}

    for r in range(a.rounds):
        segs = [rng.choice(SEGMENTS) for _ in range(rng.randint(1, 5))]
        thin = rng.random() < 0.5
        seg_bytes = [packs[s] for s in segs]
        seg_maps = [maps[s] for s in segs]
        in_set = {o for m in seg_maps for o in m.values()}
        prefix = []
        if not thin:
            for seg in seg_bytes:
                for e in packlib.parse_pack(seg)[1]:
                    if e.type == packlib.OBJ_REF_DELTA and e.base_oid not in in_set and all(p[0] != e.base_oid for p in prefix):
                        t, data = origin[e.base_oid]
                        prefix.append((e.base_oid, t, data))
        out, st = packlib.stitch_topo(seg_bytes, seg_maps, allow_thin=thin, prefix=prefix)
        stats["rounds"] += 1
        stats["thin" if thin else "nonthin"] += 1
        stats["with_duplicates"] += st["dropped_duplicates"] > 0
        stats["with_forward_refs"] += st["moved_after_base"] > 0
        label = f"round {r}: segments={'+'.join(segs)} thin={thin}"

        # structural checks
        if st["dropped_duplicates"] == 0 and st["moved_after_base"] == 0 and not prefix:
            assert out == packlib.stitch_plain(seg_bytes), label + ": topo differs from plain with nothing to fix"
            stats["identical_to_plain"] = stats.get("identical_to_plain", 0) + 1
        assert ms.shadowed_thin_bases(out, origin) == 0, label + ": hidden delta cycle"
        _, entries = packlib.parse_pack(out)
        assert len(entries) == len(in_set | {p[0] for p in prefix}), label + ": wrong object count"

        for gitbin in gits:
            repo = os.path.join(work, "check")
            shutil.rmtree(repo, ignore_errors=True)
            subprocess.run([gitbin, "init", "-q", repo], env=env, check=True)
            # The client has everything up to C5. index-pack --strict also checks
            # that every object the pack links to exists, so random plans need
            # this even when the pack is not thin; the structural check below
            # proves non-thin packs never use an external base.
            for ext in ("pack", "idx"):
                shutil.copy(os.path.join(work, "packs", f"AB.{ext}"), os.path.join(repo, ".git", "objects", "pack"))
            p = subprocess.run([gitbin, "-C", repo, "index-pack", "--strict", "--fix-thin", "--stdin"],
                               input=out, env=env, capture_output=True)
            assert p.returncode == 0, f"{label}: {gitbin}: {p.stderr.decode().strip()}"
            if gitbin == gits[-1]:
                name = p.stdout.decode().split()[-1]
                idx = os.path.join(repo, ".git", "objects", "pack", f"pack-{name}.idx")
                listing = subprocess.run([gitbin, "show-index"], stdin=open(idx, "rb"), env=env,
                                         capture_output=True, check=True).stdout.decode().splitlines()
                off_oid = {int(line.split()[0]): bytes.fromhex(line.split()[1]) for line in listing}
                oids_in_pack = [off_oid[e.offset] for e in entries]
                assert len(oids_in_pack) == len(set(oids_in_pack)), label + ": duplicate object ids"
                assert set(oids_in_pack) == in_set | {p[0] for p in prefix}, label + ": object set differs"
                pos = {o: i for i, o in enumerate(oids_in_pack)}
                for i, e in enumerate(entries):
                    if e.type == packlib.OBJ_REF_DELTA:
                        b = e.base_oid
                        if b in pos:
                            assert pos[b] < i, label + ": REF_DELTA before its base"
                        else:
                            assert thin, label + ": external base in a non-thin pack"
    print(f"check_topo: OK {stats}")
    check_single_pass(g, packs, maps, scratch)


def check_single_pass(g, packs, maps, scratch) -> None:
    """With self-contained, base-first segments, keep-first dedupe never needs a reorder.

    Makes base-first versions of the completed push packs (Bsc, Csc: bases
    moved in front of the deltas that use them), then stitches every sequence
    of 1-3 base-first segments, in any order and with repeats, and asserts the
    stitcher emitted everything in one pass without moving any entry. That is
    the property that lets A2 stream a stitched pack in a single pass.
    """
    import itertools

    names = ["A", "AB", "FULL"]
    for n in ("Bsc", "Csc"):
        out, _ = packlib.stitch_topo([packs[n]], [maps[n]], allow_thin=False)
        path = os.path.join(scratch, f"{n}-basefirst.pack")
        with open(path, "wb") as f:
            f.write(out)
        packs[n + "-bf"] = out
        maps[n + "-bf"] = ms.oid_map(g, path, False, scratch)
        names.append(n + "-bf")
    count = 0
    for k in range(1, 4):
        for combo in itertools.product(names, repeat=k):
            _, st = packlib.stitch_topo([packs[c] for c in combo], [maps[c] for c in combo], allow_thin=False)
            assert st["passes"] == 1 and st["moved_after_base"] == 0, f"{'+'.join(combo)}: {st}"
            count += 1
    print(f"check_single_pass: OK, {count} sequences of base-first segments stitched in one pass")


if __name__ == "__main__":
    main()
