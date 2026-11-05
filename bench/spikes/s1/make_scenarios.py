#!/usr/bin/env python3
"""Build the S1 scenarios: segment packs, stitched packs, client seed packs.

The origin repository has six commits C1..C6 on main. Packs are cut the way
an Aspen server would hold them:

  A    = C1..C3, full (the first push)
  B    = C4..C5, thin against C3 (a later push: REF_DELTA bases live in A)
  C    = C6,     thin against C5
  AB   = C1..C5, full (a compacted cover over A and B)
  FULL = C1..C6, full (a compacted cover over everything)
  Bsc, Csc = B and C completed with `index-pack --fix-thin`: the missing
         bases appended as full objects. This is what Aspen actually stores
         (docs/PLAN.md: every stored pack is self-contained, spike S3), and
         the appended bases are copies of objects already in earlier packs.

Each scenario stitches some of those segments and says which pack the client
already has. Everything is deterministic: fixed content seed, fixed dates,
single-threaded pack-objects.

Usage: make_scenarios.py --git <path to reference git> --out <dir>
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import struct
import subprocess
import hashlib
import zlib

import packlib

HERE = os.path.dirname(os.path.abspath(__file__))

# name, description, segments, stitcher, client seed pack, expected outcome
SCENARIOS = [
    # Plain stitching: the algorithm as written in docs/PLAN.md.
    ("control-single", "Control: one ordinary full pack, no stitching.", ["FULL"], "plain", None, "accept"),
    ("control-bad-count", "Negative control: header count off by one. Every client must reject it.", ["FULL"], "badcount", None, "reject"),
    ("clone-3seg", "Clone from three stored packs A+B+C. B and C are thin, so their REF_DELTA bases are in earlier segments.", ["A", "B", "C"], "plain", None, "accept"),
    ("thin-2seg", "Incremental fetch: client has C3, gets B+C. Some REF_DELTA bases exist only in the client's repository (thin pack).", ["B", "C"], "plain", "A", "accept"),
    ("reverse-order", "Segments in reverse order C+B+A: REF_DELTA bases appear after the deltas that need them.", ["C", "B", "A"], "plain", None, "accept"),
    ("overfetch-have", "Client has C5 but is sent A+B+C: every object of A and B is already in the client's repository.", ["A", "B", "C"], "plain", "AB", "accept"),
    ("dup-overlap", "Overlapping cover: AB+B+C. Objects of B appear twice, encoded differently (full/OFS_DELTA in AB, REF_DELTA in B).", ["AB", "B", "C"], "plain", None, "?"),
    ("dup-exact", "The same segment twice: A+A+B+C. Every object of A appears twice with identical bytes.", ["A", "A", "B", "C"], "plain", None, "?"),
    ("dup-thin", "Client has C3, gets B+FULL. Objects of B appear twice, and B's thin bases are also in FULL.", ["B", "FULL"], "plain", "A", "?"),
    # What Aspen stores after S3: self-contained push packs whose completed bases duplicate older packs.
    ("sc-single", "One stored self-contained push pack (Bsc) sent as-is, client has C3. No stitching; its completed bases sit after the deltas that use them.", ["Bsc"], "plain", "A", "?"),
    ("sc-clone", "Clone from self-contained push packs A+Bsc+Csc (as stored after S3). Completed bases duplicate objects of earlier packs.", ["A", "Bsc", "Csc"], "plain", None, "?"),
    ("sc-fetch", "Client has C3, gets Bsc+Csc. Completed bases duplicate objects the client has and objects of Bsc.", ["Bsc", "Csc"], "plain", "A", "?"),
    # Negative result: dropping later duplicates naively creates delta cycles.
    ("dup-thin+naive", "dup-thin with naive dedupe (keep first copy, OFS->REF in touched segments). Creates 2-object delta cycles.", ["B", "FULL"], "naive", "A", "reject"),
    # Recommended stitcher: drop duplicates, emit bases before deltas, rewrite headers only.
    ("reverse-order+topo", "reverse-order with the topological stitcher (deltas moved after their bases).", ["C", "B", "A"], "topo", None, "accept"),
    ("dup-overlap+topo", "dup-overlap with the topological stitcher (duplicates dropped).", ["AB", "B", "C"], "topo", None, "accept"),
    ("dup-exact+topo", "dup-exact with the topological stitcher.", ["A", "A", "B", "C"], "topo", None, "accept"),
    ("dup-thin+topo", "dup-thin with the topological stitcher: no cycles, still thin.", ["B", "FULL"], "topo", "A", "accept"),
    ("sc-single+topo", "sc-single through the topological stitcher (bases moved first).", ["Bsc"], "topo", "A", "accept"),
    ("sc-clone+topo", "sc-clone with the topological stitcher.", ["A", "Bsc", "Csc"], "topo", None, "accept"),
    ("sc-fetch+topo", "sc-fetch with the topological stitcher.", ["Bsc", "Csc"], "topo", "A", "accept"),
    ("thin-2seg+bases", "thin-2seg for a client without thin-pack: external bases sent whole in front.", ["B", "C"], "topo+bases", "A", "accept"),
]

PACKS = {
    # name: (revs fed to pack-objects --revs, thin?)
    "A": (["C3"], False),
    "B": (["C5", "^C3"], True),
    "C": (["C6", "^C5"], True),
    "AB": (["C5"], False),
    "FULL": (["C6"], False),
}


class Git:
    def __init__(self, git: str, cwd: str):
        self.git, self.cwd = git, cwd
        self.env = dict(
            os.environ,
            GIT_AUTHOR_NAME="Aspen S1",
            GIT_AUTHOR_EMAIL="s1@aspen.invalid",
            GIT_COMMITTER_NAME="Aspen S1",
            GIT_COMMITTER_EMAIL="s1@aspen.invalid",
            GIT_CONFIG_NOSYSTEM="1",
            HOME=cwd,
        )

    def __call__(self, *args: str, input: bytes | None = None, date: int | None = None) -> bytes:
        env = self.env
        if date is not None:
            env = dict(env, GIT_AUTHOR_DATE=f"{date} +0000", GIT_COMMITTER_DATE=f"{date} +0000")
        return subprocess.run(
            [self.git, "-c", "pack.threads=1", "-c", "core.bigFileThreshold=1g", *args],
            cwd=self.cwd, input=input, env=env, check=True, capture_output=True,
        ).stdout


def build_origin(git_bin: str, path: str) -> dict[str, str]:
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    g = Git(git_bin, path)
    g("init", "-q", "--initial-branch=main")
    rng = random.Random(1)
    words = [f"w{i:04d}" for i in range(4000)]
    files = {f"src/f{i:02d}.txt": [" ".join(rng.choices(words, k=12)) for _ in range(300)] for i in range(24)}

    def write_all():
        for name, lines in files.items():
            os.makedirs(os.path.join(path, os.path.dirname(name)), exist_ok=True)
            with open(os.path.join(path, name), "w") as f:
                f.write("\n".join(lines) + "\n")

    commits = {}
    for n in range(1, 7):
        if n > 1:
            for name in rng.sample(sorted(files), 6):
                for _ in range(4):
                    files[name][rng.randrange(300)] = " ".join(rng.choices(words, k=12))
        if n == 6:  # a new file that deltas well against an old one
            files["src/copy.txt"] = list(files["src/f00.txt"])
            files["src/copy.txt"][0] = "changed first line"
        write_all()
        g("add", "-A")
        g("commit", "-q", "-m", f"C{n}", date=1_700_000_000 + n * 60)
        commits[f"C{n}"] = g("rev-parse", "HEAD").decode().strip()
    return commits


def oid_map(g: Git, pack_path: str, thin: bool, scratch: str) -> dict[int, bytes]:
    """Map entry offset -> object id, using stock git index-pack."""
    with open(pack_path, "rb") as f:
        data = f.read()
    out = os.path.join(scratch, os.path.basename(pack_path) + ".fixed.pack")
    if thin:
        g("index-pack", "--stdin", "--fix-thin", out, input=data)
    else:
        shutil.copy(pack_path, out)
        g("index-pack", out)
    idx = out[:-5] + ".idx"
    m = {}
    end = len(data) - 20
    for line in g("show-index", input=open(idx, "rb").read()).decode().splitlines():
        off, oid = line.split()[:2]
        if int(off) < end:  # --fix-thin appends bases after the original entries
            m[int(off)] = bytes.fromhex(oid)
    return m


def analyse(segments: list[bytes], maps: list[dict[int, bytes]], stitched_oids: list[bytes]) -> dict:
    """Count the properties each scenario claims to exercise."""
    in_pack = set(stitched_oids)
    seg_oids = [set(m.values()) for m in maps]
    first_pos = {}
    pos = 0
    for m in maps:
        for off in sorted(m):
            first_pos.setdefault(m[off], pos)
            pos += 1
    cross = thin = forward = 0
    pos = 0
    for i, (seg, m) in enumerate(zip(segments, maps)):
        _, entries = packlib.parse_pack(seg)
        for e in entries:
            if e.type == packlib.OBJ_REF_DELTA:
                b = e.base_oid
                if b not in in_pack:
                    thin += 1
                else:
                    if b not in seg_oids[i]:
                        cross += 1
                    if first_pos[b] > pos:  # also within one segment: --fix-thin appends bases at the end
                        forward += 1
            pos += 1
    total = sum(len(m) for m in maps)
    return {
        "entries": total,
        "duplicate_entries": total - len(in_pack),
        "cross_segment_ref_deltas": cross,
        "forward_ref_deltas": forward,
        "thin_ref_deltas": thin,
    }


def typ_name(t: int) -> str:
    return packlib.TYPE_NAMES[t]


def shadowed_thin_bases(pack: bytes, external: dict[bytes, tuple[int, bytes]]) -> int:
    """Count deltas that can only be resolved through an external base that is also in the pack.

    Resolve the pack like index-pack --fix-thin: in-pack bases first, then
    external objects (`external`: oid -> (type, content), the client's
    repository). A REF_DELTA resolved through an external object whose id the
    pack also contains means the pack's own copy of that object depends,
    directly or not, on this delta: a delta cycle that --fix-thin papers over.
    """
    _, entries = packlib.parse_pack(pack)
    content: dict[int, tuple[int, bytes]] = {}
    oid_off: dict[bytes, int] = {}
    via_external: set[int] = set()

    def done(e, t, data):
        content[e.offset] = (t, data)
        oid = hashlib.sha1(b"%s %d\0" % (typ_name(t).encode(), len(data)) + data).digest()
        oid_off.setdefault(oid, e.offset)

    for e in entries:
        if e.type in (1, 2, 3, 4):
            done(e, e.type, zlib.decompress(pack[e.data_start:e.end]))
    use_external = False
    while True:
        changed = False
        for e in entries:
            if e.offset in content:
                continue
            if e.type == 6:
                base = content.get(e.base_offset)
            else:
                base = content.get(oid_off.get(e.base_oid, -1))
                if base is None and use_external and e.base_oid in external:
                    base = external[e.base_oid]
                    via_external.add(e.offset)
            if base is None:
                continue
            done(e, base[0], apply_delta(base[1], zlib.decompress(pack[e.data_start:e.end])))
            changed = True
        if not changed:
            if use_external:
                break
            use_external = True
    if len(content) != len(entries):
        raise packlib.PackError(f"{len(entries) - len(content)} entries unresolvable")
    return sum(1 for e in entries if e.offset in via_external and e.base_oid in oid_off)


def apply_delta(base: bytes, delta: bytes) -> bytes:
    def varint(i):
        n = s = 0
        while True:
            c = delta[i]; i += 1
            n |= (c & 0x7F) << s; s += 7
            if not c & 0x80:
                return n, i
    src, i = varint(0)
    dst, i = varint(i)
    if src != len(base):
        raise packlib.PackError("delta base size mismatch")
    out = bytearray()
    while i < len(delta):
        c = delta[i]; i += 1
        if c & 0x80:
            off = size = 0
            for b in range(4):
                if c & (1 << b):
                    off |= delta[i] << (8 * b); i += 1
            for b in range(3):
                if c & (0x10 << b):
                    size |= delta[i] << (8 * b); i += 1
            size = size or 0x10000
            out += base[off:off + size]
        elif c:
            out += delta[i:i + c]; i += c
        else:
            raise packlib.PackError("delta opcode 0")
    if len(out) != dst:
        raise packlib.PackError("delta result size mismatch")
    return bytes(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--git", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    origin = os.path.join(out, "origin")
    packs_dir = os.path.join(out, "packs")
    scratch = os.path.join(out, "scratch")
    for d in (packs_dir, scratch, os.path.join(out, "scenarios")):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
    commits = build_origin(a.git, origin)
    g = Git(a.git, origin)

    seg_bytes, seg_maps = {}, {}
    for name, (revs, thin) in PACKS.items():
        args = ["pack-objects", "--revs", "--stdout", "--delta-base-offset"] + (["--thin"] if thin else [])
        revtext = "".join(("^" + commits[r[1:]] if r.startswith("^") else commits[r]) + "\n" for r in revs)
        data = g(*args, input=revtext.encode())
        path = os.path.join(packs_dir, f"{name}.pack")
        with open(path, "wb") as f:
            f.write(data)
        seg_bytes[name] = data
        seg_maps[name] = oid_map(g, path, thin, scratch)
        if not thin:  # seed packs for clients: pack + idx made by stock git
            g("index-pack", path)
        else:  # the self-contained version Aspen would store
            sc = os.path.join(packs_dir, f"{name}sc.pack")
            g("index-pack", "--stdin", "--fix-thin", sc, input=data)
            seg_bytes[name + "sc"] = open(sc, "rb").read()
            seg_maps[name + "sc"] = oid_map(g, sc, False, scratch)

    origin_objects = {}
    for line in g("cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)").decode().splitlines():
        h, t = line.split()
        origin_objects[bytes.fromhex(h)] = ({"commit": 1, "tree": 2, "blob": 3, "tag": 4}[t], g("cat-file", t, h))
    summary = {"commits": commits, "packs": {}, "scenarios": []}
    for name, data in seg_bytes.items():
        summary["packs"][name] = packlib.describe(data)

    def external_bases(segments, maps):
        inset = {o for m in maps for o in m.values()}
        need = []
        for seg in segments:
            for e in packlib.parse_pack(seg)[1]:
                if e.type == packlib.OBJ_REF_DELTA and e.base_oid not in inset and e.base_oid not in need:
                    need.append(e.base_oid)
        out = []
        for oid in need:
            h = oid.hex()
            typ = {"commit": 1, "tree": 2, "blob": 3, "tag": 4}[g("cat-file", "-t", h).decode().strip()]
            out.append((oid, typ, g("cat-file", typ_name(typ), h)))
        return out

    for name, desc, segs, stitcher, seed, expect in SCENARIOS:
        d = os.path.join(out, "scenarios", name)
        os.makedirs(d)
        segments = [seg_bytes[s] for s in segs]
        maps = [seg_maps[s] for s in segs]
        all_oids = [m[o] for m in maps for o in sorted(m)]
        # The tip is the newest commit the stitched pack carries.
        tip = next(commits[c] for c in sorted(commits, reverse=True) if bytes.fromhex(commits[c]) in set(all_oids))
        expected = len(g("rev-list", "--objects", tip).decode().splitlines())
        stats = None
        if stitcher == "plain":
            stitched = packlib.stitch_plain(segments)
        elif stitcher == "naive":
            stitched, dropped = packlib.stitch_naive_dedupe(segments, maps)
            stats = {"dropped_duplicates": dropped}
        elif stitcher == "topo":
            stitched, stats = packlib.stitch_topo(segments, maps, allow_thin=True)
        elif stitcher == "topo+bases":
            stitched, stats = packlib.stitch_topo(segments, maps, allow_thin=False, prefix=external_bases(segments, maps))
        elif stitcher == "badcount":
            base = packlib.stitch_plain(segments)
            count = struct.unpack(">I", base[8:12])[0] + 1
            body = base[:8] + struct.pack(">I", count) + base[12:-20]
            stitched = body + hashlib.sha1(body).digest()
        with open(os.path.join(d, "stitched.pack"), "wb") as f:
            f.write(stitched)
        props = analyse(segments, maps, all_oids)
        if stitcher != "badcount":
            props["shadowed_thin_bases"] = shadowed_thin_bases(stitched, origin_objects)
        if stats:
            props["stitcher"] = stats
        meta = {
            "name": name,
            "description": desc,
            "segments": segs,
            "stitcher": stitcher,
            "client_has": seed,
            "client_tip": {"A": commits["C3"], "AB": commits["C5"]}.get(seed),
            "tip": tip,
            "head_ref": "refs/heads/main",
            "expected_objects": expected,
            "expect": expect,
            "stitched_bytes": len(stitched),
            "properties": props,
        }
        with open(os.path.join(d, "scenario.json"), "w") as f:
            json.dump(meta, f, indent=2)
        summary["scenarios"].append(meta)
    with open(os.path.join(out, "scenarios.json"), "w") as f:
        json.dump(summary, f, indent=2)
    for s in summary["scenarios"]:
        print(f"{s['name']:20} {s['stitched_bytes']:7}B {s['properties']}")


if __name__ == "__main__":
    main()
