#!/usr/bin/env python3
"""Spike S3: run the packfile-uri scenario matrix against several git clients.

Usage:
    python3 matrix.py [--clients 2.30,2.39,...] [--only B1,E1] [--out results]

Clients are looked up as $GITS_DIR/<name>/bin/git (built by build-git.sh),
except "system", which is the git on PATH. Results are written to
<out>/results.json (every observation) and <out>/matrix.md (the summary
table). See README.md in this directory for what each scenario means.

Only the Python standard library and git are needed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import zlib
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import server  # noqa: E402

GITS_DIR = Path(os.environ.get("GITS_DIR", "/tmp/gits"))
SYSTEM_GIT = shutil.which("git")
REPO_HOST = "127.0.0.1"   # where the Git endpoint is reached
BLOB_HOST = "127.0.0.2"   # a different host name, standing in for a CDN/bucket


# --------------------------------------------------------------------------
# fixtures


def sh(*args, cwd=None, input=None, env=None) -> bytes:
    return subprocess.run(args, cwd=cwd, input=input, env=env, check=True,
                          stdout=subprocess.PIPE).stdout


def pack_entry_types(path: Path) -> Counter:
    """Count entry types in a pack: commit/tree/blob/tag/ofs_delta/ref_delta."""
    names = {1: "commit", 2: "tree", 3: "blob", 4: "tag", 6: "ofs_delta", 7: "ref_delta"}
    data = path.read_bytes()
    count = int.from_bytes(data[8:12], "big")
    pos, types = 12, Counter()
    for _ in range(count):
        c = data[pos]
        kind = (c >> 4) & 7
        pos += 1
        while c & 0x80:
            c = data[pos]
            pos += 1
        if kind == 6:
            c = data[pos]
            pos += 1
            while c & 0x80:
                c = data[pos]
                pos += 1
        elif kind == 7:
            pos += 20
        d = zlib.decompressobj()
        d.decompress(data[pos:])
        pos = len(data) - len(d.unused_data)
        types[names[kind]] += 1
    return types


class Fixtures:
    """A backing repository with a small history and packs cut from it."""

    def __init__(self, root: Path):
        self.root = root
        self.backing = root / "backing.git"
        self.packs_dir = root / "packs"
        self.commits: dict[str, str] = {}
        self.packs: dict[str, dict] = {}

    def build(self):
        if self.root.exists():
            shutil.rmtree(self.root)
        self.packs_dir.mkdir(parents=True)
        work = self.root / "work"
        env = dict(os.environ, GIT_AUTHOR_NAME="Spike", GIT_AUTHOR_EMAIL="spike@example.com",
                   GIT_COMMITTER_NAME="Spike", GIT_COMMITTER_EMAIL="spike@example.com",
                   GIT_AUTHOR_DATE="2026-11-03T00:00:00Z",
                   GIT_COMMITTER_DATE="2026-11-03T00:00:00Z")
        sh("git", "init", "-q", "-b", "main", str(work), env=env)

        def commit(label: str, mutate):
            mutate()
            sh("git", "add", "-A", cwd=work, env=env)
            sh("git", "commit", "-q", "-m", label, cwd=work, env=env)
            self.commits[label] = sh("git", "rev-parse", "HEAD", cwd=work).decode().strip()

        # A large, line-oriented text file so later versions delta against it.
        big = [f"line {i:06d} " + "lorem ipsum dolor sit amet " * 3 for i in range(8000)]

        def c1():
            (work / "big.txt").write_text("\n".join(big) + "\n")
            (work / "src").mkdir()
            for i in range(40):
                (work / "src" / f"f{i:02d}.txt").write_text(f"file {i}\n" * 50)

        def c2():
            big[100] = "line changed in c2"
            big.append("appended in c2")
            (work / "big.txt").write_text("\n".join(big) + "\n")
            (work / "src" / "new-in-c2.txt").write_text("c2\n")

        def c3():
            big[4000] = "line changed in c3"
            (work / "big.txt").write_text("\n".join(big) + "\n")
            (work / "src" / "f00.txt").write_text("rewritten in c3\n" * 50)

        commit("C1", c1)
        commit("C2", c2)
        commit("C3", c3)
        # 50 small commits on top of C3 for the many-URIs scenario.
        sh("git", "checkout", "-q", "-b", "many", cwd=work)
        for i in range(1, 51):
            commit(f"M{i}", lambda i=i: (work / "many.txt").write_text(f"step {i}\n"))
        sh("git", "clone", "-q", "--bare", str(work), str(self.backing))
        # Only main is advertised; scenarios move it. The M* objects stay reachable
        # through a hidden ref so the backing repository can still serve them.
        sh("git", "-C", str(self.backing), "update-ref", "refs/hidden/many", "refs/heads/many")
        sh("git", "-C", str(self.backing), "update-ref", "-d", "refs/heads/many")
        sh("git", "-C", str(self.backing), "config", "uploadpack.hideRefs", "refs/hidden")
        sh("git", "-C", str(self.backing), "config", "uploadpack.allowAnySHA1InWant", "true")

        c = self.commits
        ofs = ["--delta-base-offset"]
        self.make_pack("full_c1", [c["C1"]], ofs)
        self.make_pack("full_c2", [c["C2"]], ofs)
        self.make_pack("full_c3", [c["C3"]], ofs)
        self.make_pack("inc_c1_c2", [c["C2"], "^" + c["C1"]], ofs)
        self.make_pack("inc_c2_c3", [c["C3"], "^" + c["C2"]], ofs)
        self.make_pack("inc_c1_c3", [c["C3"], "^" + c["C1"]], ofs)
        self.make_pack("thin_c1_c3", [c["C3"], "^" + c["C1"]], ofs + ["--thin"])
        self.make_pack("refdelta_full_c3", [c["C3"]], [])  # REF_DELTA only, self-contained
        prev = c["C3"]
        for i in range(1, 51):
            self.make_pack(f"many_{i:02d}", [c[f"M{i}"], "^" + prev], ofs)
            prev = c[f"M{i}"]

        # Prove the thin pack really is thin: strict indexing alone rejects it.
        scratch = self.root / "scratch.git"
        sh("git", "init", "-q", "--bare", str(scratch))
        r = subprocess.run(["git", "-C", str(scratch), "index-pack", "--stdin"],
                           input=(self.packs_dir / "thin_c1_c3.pack").read_bytes(),
                           capture_output=True)
        assert r.returncode != 0, "thin_c1_c3 is not thin; fixture is wrong"
        shutil.rmtree(scratch)
        shutil.rmtree(work)

    def make_pack(self, name: str, revs: list[str], opts: list[str]):
        data = sh("git", "-C", str(self.backing), "pack-objects", "--revs", "--stdout",
                  "-q", *opts, input=("\n".join(revs) + "\n").encode())
        path = self.packs_dir / f"{name}.pack"
        path.write_bytes(data)
        self.packs[name] = {
            "hash": data[-20:].hex(),
            "size": len(data),
            "objects": int.from_bytes(data[8:12], "big"),
            "entry_types": dict(pack_entry_types(path)),
        }

    def set_ref(self, name: str, label: str):
        sh("git", "-C", str(self.backing), "update-ref", name, self.commits[label])


# --------------------------------------------------------------------------
# running clients


class Client:
    def __init__(self, name: str, root: Path):
        self.name = name
        self.git = SYSTEM_GIT if name == "system" else str(GITS_DIR / name / "bin" / "git")
        self.home = root / f"home-{name}"
        self.home.mkdir(parents=True, exist_ok=True)
        (self.home / ".gitconfig").write_text(
            "[user]\n\tname = Spike\n\temail = spike@example.com\n"
            "[init]\n\tdefaultBranch = main\n[advice]\n\tdetachedHead = false\n"
            # Background auto-gc/maintenance would race the fsck we run after
            # each step and rewrite pack directories under us.
            "[gc]\n\tauto = 0\n[maintenance]\n\tauto = false\n")
        self.version = subprocess.run([self.git, "--version"], capture_output=True,
                                      text=True).stdout.strip()

    def env(self) -> dict:
        # Drop GIT_* and any proxy settings: every URL in the spike is local.
        env = {k: v for k, v in os.environ.items()
               if not k.startswith("GIT_") and "proxy" not in k.lower()}
        env.update(HOME=str(self.home), GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0",
                   PATH=str(Path(self.git).parent) + ":" + os.environ["PATH"], LC_ALL="C")
        return env

    def run(self, args: list[str], cwd: Path | None = None, timeout: float = 60,
            interrupt_after: float | None = None, sig: int = signal.SIGINT) -> dict:
        t0 = time.monotonic()
        p = subprocess.Popen([self.git, *args], cwd=cwd, env=self.env(), text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True)
        try:
            if interrupt_after is not None:
                try:
                    out, err = p.communicate(timeout=interrupt_after)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, sig)  # SIGINT is what Ctrl-C does
                    out, err = p.communicate(timeout=15)
            else:
                out, err = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            out, err = p.communicate()
            return {"rc": "timeout", "err": err, "secs": round(time.monotonic() - t0, 2)}
        return {"rc": p.returncode, "out": out, "err": err,
                "secs": round(time.monotonic() - t0, 2)}


def first_error(err: str) -> str:
    for line in err.splitlines():
        line = line.strip()
        if line.startswith(("fatal:", "error:")):
            return line[:160]
    lines = [l for l in err.splitlines() if l.strip()]
    return (lines[-1][:160] if lines else "")


# --------------------------------------------------------------------------
# scenario context


class Ctx:
    def __init__(self, fx: Fixtures, client: Client, state_root: Path, port: int, sid: str):
        self.fx, self.client, self.state_root, self.port, self.sid = fx, client, state_root, port, sid
        self.work = state_root / "runs" / client.name / sid
        if self.work.exists():
            shutil.rmtree(self.work)
        self.work.mkdir(parents=True)
        self.repo_url = f"http://{REPO_HOST}:{port}/repo.git"
        self.obs: dict = {}

    # scenario plumbing

    def blob_url(self, pack: str, host: str = BLOB_HOST, query: str = "") -> str:
        return f"http://{host}:{self.port}/blobs/{pack}.pack" + (f"?{query}" if query else "")

    def uri(self, pack: str, **kw) -> dict:
        return {"hash": self.fx.packs[pack]["hash"], "url": self.blob_url(pack, **kw)}

    def scenario(self, **sc):
        path = self.state_root / "scenario.json"
        path.write_text(json.dumps(sc))
        # Bump mtime so the server resets per-blob attempt counters.
        t = time.time_ns()
        os.utime(path, ns=(t, t))
        time.sleep(0.01)

    def mark(self) -> int:
        log = self.state_root / "requests.jsonl"
        return log.stat().st_size if log.exists() else 0

    def log_since(self, mark: int) -> list[dict]:
        with open(self.state_root / "requests.jsonl") as f:
            f.seek(mark)
            return [json.loads(l) for l in f if l.strip()]

    # git operations

    def git(self, *args, cwd=None, **kw) -> dict:
        return self.client.run(list(args), cwd=cwd, **kw)

    def clone(self, dest="clone", uriprotocols: str | None = "http", extra=(), **kw) -> dict:
        cfg = ["-c", f"fetch.uriprotocols={uriprotocols}"] if uriprotocols else []
        return self.step("clone", self.git(*cfg, *extra, "clone", "-q", self.repo_url,
                                           str(self.work / dest), **kw), dest)

    def fetch(self, dest="clone", uriprotocols: str | None = "http", extra=(), label="fetch",
              **kw) -> dict:
        cfg = ["-c", f"fetch.uriprotocols={uriprotocols}"] if uriprotocols else []
        return self.step(label, self.git(*cfg, *extra, "-C", str(self.work / dest), "fetch",
                                         "-q", "origin", **kw), dest)

    def base_clone_at(self, label: str, dest="clone"):
        """Plain clone (no packfile URIs) with main at `label`."""
        self.fx.set_ref("refs/heads/main", label)
        self.scenario()
        r = self.git("clone", "-q", self.repo_url, str(self.work / dest))
        assert r["rc"] == 0, f"base clone failed: {r['err']}"

    def step(self, label: str, r: dict, dest: str) -> dict:
        repo = self.work / dest
        info = {"rc": r["rc"], "secs": r["secs"]}
        if r["rc"] != 0:
            info["error"] = first_error(r["err"])
        info["exists"] = repo.exists()
        if repo.exists():
            pack_dir = repo / ".git" / "objects" / "pack"
            files = sorted(p.name for p in pack_dir.iterdir()) if pack_dir.exists() else []
            info["packs"] = sum(f.endswith(".pack") for f in files)
            info["temp"] = [f"{f} ({(pack_dir / f).stat().st_size} B)" for f in files
                            if f.endswith(".temp")]
            info["keep"] = [f for f in files if f.endswith(".keep")]
            tip = self.git("-C", str(repo), "rev-parse", "-q", "--verify", "refs/remotes/origin/main")
            tip = tip.get("out", "").strip()
            info["origin_main"] = next((k for k, v in self.fx.commits.items() if v == tip),
                                       tip[:12] or None)
            if r["rc"] == 0:
                f = self.git("-C", str(repo), "fsck", "--strict", "--no-dangling", timeout=120)
                info["fsck"] = "ok" if f["rc"] == 0 else first_error(f["err"])
        self.obs[label] = info
        return info

    def temp_path(self, pack: str, dest="clone") -> Path:
        h = self.fx.packs[pack]["hash"]
        return self.work / dest / ".git" / "objects" / "pack" / f"pack-{h}.pack.temp"


# --------------------------------------------------------------------------
# scenarios
#
# Each returns a one-line summary for the matrix. Details go in ctx.obs.

SCENARIOS: dict[str, tuple[str, callable]] = {}


def scenario(sid: str, title: str):
    def wrap(fn):
        SCENARIOS[sid] = (title, fn)
        return fn
    return wrap


def ok(info: dict, want: str) -> bool:
    return info["rc"] == 0 and info.get("origin_main") == want and info.get("fsck") == "ok"


def summarize(info: dict, want: str | None = None) -> str:
    if info["rc"] == 0:
        s = "OK" if want is None or info.get("origin_main") == want else f"WRONG TIP {info.get('origin_main')}"
        if info.get("fsck") not in (None, "ok"):
            s += f" (fsck: {info['fsck']})"
        return s
    return "FAIL: " + (info.get("error") or f"rc={info['rc']}")


def leftovers(info: dict) -> str:
    bits = []
    if info.get("temp"):
        sizes = ", ".join(t.rsplit("(", 1)[1].rstrip(")") for t in info["temp"])
        bits.append(f"{len(info['temp'])} .temp ({sizes})")
    if info.get("keep"):
        bits.append(f"{len(info['keep'])} .keep")
    return (" [left: " + ", ".join(bits) + "]") if bits else ""


def uris_requested(log: list[dict]) -> str:
    fetches = [e for e in log if e.get("kind") == "upload_pack" and e.get("command") in ("fetch", "v0")]
    args = [a for e in fetches for a in e.get("args", []) if a.startswith("packfile-uris")]
    return args[0] if args else "not requested"


# --- A: opt-in and capability negotiation


@scenario("A1", "clone, fetch.uriprotocols unset")
def a1(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")])
    m = ctx.mark()
    info = ctx.clone(uriprotocols=None)
    return f"{summarize(info, 'C3')}; client {uris_requested(ctx.log_since(m))}"


@scenario("A2", "clone, uriprotocols=http but protocol.version=0")
def a2(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")])
    m = ctx.mark()
    info = ctx.clone(extra=["-c", "protocol.version=0"])
    return f"{summarize(info, 'C3')}; client {uris_requested(ctx.log_since(m))}"


@scenario("A3", "clone, server does not advertise packfile-uris")
def a3(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")], advertise_packfile_uris=False)
    m = ctx.mark()
    info = ctx.clone()
    return f"{summarize(info, 'C3')}; client {uris_requested(ctx.log_since(m))}"


@scenario("A4", "clone, client allows https only, server sends an http URI")
def a4(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")])
    m = ctx.mark()
    info = ctx.clone(uriprotocols="https")
    log = ctx.log_since(m)
    hits = sum(e["kind"] == "blob" for e in log)
    return f"{summarize(info, 'C3')}; client {uris_requested(log)}; blob requests={hits}"


# --- B: happy paths and plan shapes


@scenario("B1", "clone, 1 self-contained URI pack + empty inline pack")
def b1(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")])
    m = ctx.mark()
    info = ctx.clone()
    return f"{summarize(info, 'C3')}; client {uris_requested(ctx.log_since(m))}{leftovers(info)}"


@scenario("B2", "clone, 3 layered URI packs (base + 2 increments)")
def b2(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c1"), ctx.uri("inc_c1_c2"), ctx.uri("inc_c2_c3")])
    info = ctx.clone()
    return summarize(info, "C3") + f"; packs={info.get('packs')}" + leftovers(info)


@scenario("B3", "fetch C1->C3 with haves, 1 incremental URI pack")
def b3(ctx: Ctx):
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("inc_c1_c3")])
    info = ctx.fetch()
    return summarize(info, "C3") + leftovers(info)


@scenario("B4", "clone, URI packs overlap (duplicate objects across packs)")
def b4(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c2"), ctx.uri("inc_c1_c3"), ctx.uri("full_c3")])
    info = ctx.clone()
    return summarize(info, "C3") + leftovers(info)


@scenario("B5", "clone, URI pack for C2 + non-empty inline pack for C2..C3")
def b5(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c2")], inline="packs/inc_c2_c3.pack")
    info = ctx.clone()
    return summarize(info, "C3") + leftovers(info)


@scenario("B6", "clone, packfile-uris section but no packfile section")
def b6(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")], inline="none")
    info = ctx.clone()
    return summarize(info, "C3") + leftovers(info)


@scenario("B7", "clone, 51 URI packs (full C3 + 50 one-commit packs)")
def b7(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "M50")
    uris = [ctx.uri("full_c3")] + [ctx.uri(f"many_{i:02d}") for i in range(1, 51)]
    ctx.scenario(uris=uris)
    info = ctx.clone(timeout=300)
    return summarize(info, "M50") + f"; {info['secs']}s" + leftovers(info)


@scenario("B8", "clone, 51 URI packs, fetch.packfileUriJobs=8 + no-ref-delta advertised")
def b8(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "M50")
    uris = [ctx.uri("full_c3")] + [ctx.uri(f"many_{i:02d}") for i in range(1, 51)]
    ctx.scenario(uris=uris, advertise_no_ref_delta=True)
    m = ctx.mark()
    info = ctx.clone(extra=["-c", "fetch.packfileUriJobs=8"], timeout=300)
    args = [a for e in ctx.log_since(m) if e.get("kind") == "upload_pack" for a in e.get("args", [])]
    nrd = "no-ref-delta requested" if "no-ref-delta" in args else "no-ref-delta not requested"
    return summarize(info, "M50") + f"; {info['secs']}s; {nrd}" + leftovers(info)


@scenario("B9", "clone, self-contained pack that uses REF_DELTA (no OFS_DELTA)")
def b9(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("refdelta_full_c3")])
    info = ctx.clone()
    return summarize(info, "C3") + leftovers(info)


@scenario("B10", "clone, URI answers 302 to the real location")
def b10(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")], blobs={"full_c3.pack": ["redirect"]})
    m = ctx.mark()
    info = ctx.clone()
    direct = sum(e["kind"] == "blob" and e["path"].startswith("/blobs-direct/") for e in ctx.log_since(m))
    return summarize(info, "C3") + f"; followed redirect={bool(direct)}" + leftovers(info)


@scenario("B11", "clone, URI answers 302; client sets http.followRedirects=true")
def b11(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")], blobs={"full_c3.pack": ["redirect"]})
    m = ctx.mark()
    info = ctx.clone(extra=["-c", "http.followRedirects=true"])
    direct = sum(e["kind"] == "blob" and e["path"].startswith("/blobs-direct/") for e in ctx.log_since(m))
    return summarize(info, "C3") + f"; followed redirect={bool(direct)}" + leftovers(info)


@scenario("B12", "3 fetches, each with 1 URI pack + a NON-empty inline pack")
def b12(ctx: Ctx):
    ctx.base_clone_at("C1")
    parts = []
    steps = [("C2", "inc_c1_c2", None), ("C3", "inc_c2_c3", None), ("M2", "many_01", "many_02")]
    for i, (tip, uri_pack, inline_pack) in enumerate(steps):
        ctx.fx.set_ref("refs/heads/main", tip)
        inline = f"packs/{inline_pack}.pack" if inline_pack else "packs/full_c1.pack"
        ctx.scenario(uris=[ctx.uri(uri_pack)], inline=inline)
        info = ctx.fetch(label=f"fetch{i + 1}")
        parts.append(f"#{i + 1}: {summarize(info, tip)}{leftovers(info)}")
    return "; ".join(parts)


# --- C: delta dependencies between packs


@scenario("C1", "fetch C1->C3, URI pack is THIN against objects the client has")
def c1(ctx: Ctx):
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("thin_c1_c3")])
    info = ctx.fetch()
    return summarize(info, "C3") + leftovers(info)


@scenario("C2", "clone, URI #2 is thin against objects in URI #1")
def c2(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c1"), ctx.uri("thin_c1_c3")])
    info = ctx.clone()
    return summarize(info, "C3") + leftovers(info)


@scenario("C3", "clone, thin URI listed BEFORE the pack holding its bases")
def c3(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("thin_c1_c3"), ctx.uri("full_c1")])
    info = ctx.clone()
    return summarize(info, "C3") + leftovers(info)


@scenario("C5", "fetch C1->C3, thin URI pack, server advertises the hash git 2.39 gets after --fix-thin")
def c5(ctx: Ctx):
    ctx.base_clone_at("C1")
    # What hash does index-pack --fix-thin produce for this thin pack, given the
    # client's objects? Compute it once with the system git in a scratch copy.
    scratch = ctx.work / "fixthin"
    shutil.copytree(ctx.work / "clone", scratch)
    r = subprocess.run([SYSTEM_GIT, "-C", str(scratch), "index-pack", "--stdin", "--fix-thin"],
                       input=(ctx.fx.packs_dir / "thin_c1_c3.pack").read_bytes(),
                       capture_output=True, check=True)
    fixed = r.stdout.decode().split()[-1]
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[{"hash": fixed, "url": ctx.blob_url("thin_c1_c3")}])
    info = ctx.fetch()
    return summarize(info, "C3") + leftovers(info)


@scenario("C6", "as C5, but the client sets core.loosecompression=9 (default is 1)")
def c6(ctx: Ctx):
    ctx.base_clone_at("C1")
    scratch = ctx.work / "fixthin"
    shutil.copytree(ctx.work / "clone", scratch)
    r = subprocess.run([SYSTEM_GIT, "-C", str(scratch), "index-pack", "--stdin", "--fix-thin"],
                       input=(ctx.fx.packs_dir / "thin_c1_c3.pack").read_bytes(),
                       capture_output=True, check=True)
    fixed = r.stdout.decode().split()[-1]
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[{"hash": fixed, "url": ctx.blob_url("thin_c1_c3")}])
    info = ctx.fetch(extra=["-c", "core.loosecompression=9"])
    return summarize(info, "C3") + leftovers(info)


@scenario("C4", "clone, jobs=4 + no-ref-delta promised, but a URI pack has REF_DELTA")
def c4(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c1"), ctx.uri("refdelta_full_c3")], advertise_no_ref_delta=True)
    info = ctx.clone(extra=["-c", "fetch.packfileUriJobs=4"])
    return summarize(info, "C3") + leftovers(info)


# --- D: integrity and HTTP failures (refs must not move)


@scenario("D1", "fetch, advertised hash does not match the pack")
def d1(ctx: Ctx):
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    u = ctx.uri("inc_c1_c3")
    u["hash"] = ctx.fx.packs["full_c1"]["hash"]
    ctx.scenario(uris=[u])
    info = ctx.fetch()
    return summarize(info, "C3") + f"; origin/main={info.get('origin_main')}" + leftovers(info)


@scenario("D2", "clone, URI packs do not cover everything (missing objects)")
def d2(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("inc_c2_c3")])
    info = ctx.clone()
    return summarize(info, "C3") + f"; dir kept={info['exists']}"


@scenario("D3", "fetch, URI returns 404")
def d3(ctx: Ctx):
    return http_failure(ctx, ["status:404"])


@scenario("D4", "fetch, URI returns 403 (expired presigned URL)")
def d4(ctx: Ctx):
    return http_failure(ctx, ["status:403"])


@scenario("D5", "fetch, URI returns 503 once, then OK")
def d5(ctx: Ctx):
    return http_failure(ctx, ["status:503", "ok"])


@scenario("D6", "fetch, URI returns 503 + Retry-After: 1 once, then OK")
def d6(ctx: Ctx):
    return http_failure(ctx, ["retry-after:1", "ok"])


@scenario("D7", "fetch, URI returns 429 once, then OK")
def d7(ctx: Ctx):
    return http_failure(ctx, ["status:429", "ok"])


def http_failure(ctx: Ctx, behaviours: list[str]) -> str:
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("inc_c1_c3")], blobs={"inc_c1_c3.pack": behaviours})
    m = ctx.mark()
    info = ctx.fetch(extra=["-c", "http.maxRetries=2", "-c", "http.retryAfter=1"])
    tries = sum(e["kind"] == "blob" for e in ctx.log_since(m))
    return summarize(info, "C3") + f"; blob requests={tries}; origin/main={info.get('origin_main')}" + leftovers(info)


@scenario("D8", "fetch, URI needs Basic auth; credential helper has creds for the blob host")
def d8(ctx: Ctx):
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    store = ctx.work / "creds"
    store.write_text(f"http://bloboperator:not-a-secret@{BLOB_HOST}:{ctx.port}\n")
    ctx.scenario(uris=[ctx.uri("inc_c1_c3")], blobs={"inc_c1_c3.pack": ["auth"]})
    m = ctx.mark()
    info = ctx.fetch(extra=["-c", f"credential.helper=store --file={store}"])
    log = [e for e in ctx.log_since(m) if e["kind"] == "blob"]
    return summarize(info, "C3") + f"; blob statuses={[e['status'] for e in log]}" + leftovers(info)


@scenario("D9", "clone with repo credentials; are they sent to a same-host blob URL?")
def d9(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    store = ctx.work / "creds"
    store.write_text(f"http://repouser:not-a-secret@{REPO_HOST}:{ctx.port}\n")
    ctx.scenario(uris=[ctx.uri("full_c3", host=REPO_HOST)], repo_auth=True)
    m = ctx.mark()
    info = ctx.clone(extra=["-c", f"credential.helper=store --file={store}"])
    blob_auth = [e["auth"] for e in ctx.log_since(m) if e["kind"] == "blob"]
    return summarize(info, "C3") + f"; Authorization sent to blob URL: {any(blob_auth)}"


# --- E: interrupted downloads and resume


@scenario("E1", "fetch: download cut at 50%, then retry against a healthy server")
def e1(ctx: Ctx):
    return resume_flow(ctx, ["truncate:0.5", "ok"])


@scenario("E2", "fetch: Ctrl-C at 50%, then retry")
def e2(ctx: Ctx):
    return resume_flow(ctx, ["stall:0.5", "ok"], interrupt=True)


@scenario("E3", "fetch: cut at 50%, retry gets a NEW presigned URL for the same pack")
def e3(ctx: Ctx):
    return resume_flow(ctx, ["truncate:0.5", "ok"], new_query_on_retry=True)


@scenario("E4", "fetch: cut at 50%, retry server ignores Range (200 + full body), then a third try")
def e4(ctx: Ctx):
    return resume_flow(ctx, ["truncate:0.5", "norange", "ok"], attempts=3)


@scenario("E5", "fetch: complete .temp already present (download done, indexing died); server answers 416")
def e5(ctx: Ctx):
    return seeded_temp_flow(ctx, ["ok"], seed="complete")


@scenario("E6", "fetch: complete .temp present; server ignores Range and sends 200 + full body")
def e6(ctx: Ctx):
    return seeded_temp_flow(ctx, ["norange"], seed="complete")


@scenario("E7", "fetch: .temp holds garbage (wrong bytes); server honours Range")
def e7(ctx: Ctx):
    return seeded_temp_flow(ctx, ["ok"], seed="garbage")


@scenario("E5b", "as E5, but the server answers a Range at EOF with 200 and an empty body")
def e5b(ctx: Ctx):
    return seeded_temp_flow(ctx, ["eof-empty-200"], seed="complete")


@scenario("E5c", "as E5, but the server answers a Range at EOF with 206 and an empty body")
def e5c(ctx: Ctx):
    return seeded_temp_flow(ctx, ["eof-empty-206"], seed="complete")


@scenario("E8", "clone: download cut at 50%, then retry the clone")
def e8(ctx: Ctx):
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("full_c3")], blobs={"full_c3.pack": ["truncate:0.5", "ok"]})
    m = ctx.mark()
    first = ctx.clone()
    second = ctx.clone()
    ranges = [e.get("range") for e in ctx.log_since(m) if e["kind"] == "blob"]
    return (f"1st: {summarize(first)}, dir kept={first['exists']}; "
            f"2nd: {summarize(second, 'C3')}; Range headers={ranges}")


@scenario("E9", "fetch: URI pack already installed locally from an earlier fetch")
def e9(ctx: Ctx):
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C2")
    ctx.scenario(uris=[ctx.uri("inc_c1_c2")])
    first = ctx.fetch(label="fetch1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    ctx.scenario(uris=[ctx.uri("inc_c1_c2"), ctx.uri("inc_c2_c3")])
    m = ctx.mark()
    second = ctx.fetch(label="fetch2")
    sent = sum(e.get("sent", 0) for e in ctx.log_since(m)
               if e["kind"] == "blob" and "inc_c1_c2" in e["path"])
    return (f"1st: {summarize(first, 'C2')}; 2nd: {summarize(second, 'C3')}; "
            f"bytes re-sent for the installed pack={sent}" + leftovers(second))


@scenario("E10", "fetch: stale pack-<hash>.keep left by a crashed fetch")
def e10(ctx: Ctx):
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    h = ctx.fx.packs["inc_c1_c3"]["hash"]
    keep = ctx.work / "clone" / ".git" / "objects" / "pack" / f"pack-{h}.keep"
    keep.write_text("left by a crashed fetch\n")
    ctx.scenario(uris=[ctx.uri("inc_c1_c3")])
    first = ctx.fetch(label="attempt1")
    second = ctx.fetch(label="attempt2")
    return f"#1: {summarize(first, 'C3')}; #2: {summarize(second, 'C3')}" + leftovers(second)


@scenario("E11", "fetch: client SIGKILLed mid-download (OOM, pod eviction), then retry")
def e11(ctx: Ctx):
    return resume_flow(ctx, ["stall:0.5", "ok"], interrupt=True, sig=signal.SIGKILL)


def resume_flow(ctx: Ctx, behaviours: list[str], interrupt=False, new_query_on_retry=False,
                attempts=2, sig=signal.SIGINT) -> str:
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    pack = "inc_c1_c3"
    size = ctx.fx.packs[pack]["size"]
    ctx.scenario(uris=[ctx.uri(pack)], blobs={f"{pack}.pack": behaviours})
    m = ctx.mark()
    parts = []
    for i in range(attempts):
        if i == 1 and new_query_on_retry:
            ctx.scenario(uris=[ctx.uri(pack, query="X-Signature=second")],
                         blobs={f"{pack}.pack": behaviours[1:]})
        kw = {"interrupt_after": 3.0, "sig": sig} if (interrupt and i == 0) else {}
        info = ctx.fetch(label=f"attempt{i + 1}", **kw)
        temp = ctx.temp_path(pack)
        tsize = temp.stat().st_size if temp.exists() else None
        parts.append(f"#{i + 1}: {summarize(info, 'C3')}, origin/main={info.get('origin_main')}, "
                     f".temp={tsize if tsize is not None else 'none'}")
        if info["rc"] == 0:
            break
    log = [e for e in ctx.log_since(m) if e["kind"] == "blob"]
    ranges = [e.get("range") for e in log]
    ctx.obs["blob_requests"] = log
    return f"pack {size} B; " + "; ".join(parts) + f"; Range headers={ranges}"


def seeded_temp_flow(ctx: Ctx, behaviours: list[str], seed: str) -> str:
    ctx.base_clone_at("C1")
    ctx.fx.set_ref("refs/heads/main", "C3")
    pack = "inc_c1_c3"
    data = (ctx.fx.packs_dir / f"{pack}.pack").read_bytes()
    temp = ctx.temp_path(pack)
    temp.write_bytes(data if seed == "complete" else b"\0" * (len(data) // 2))
    ctx.scenario(uris=[ctx.uri(pack)], blobs={f"{pack}.pack": behaviours})
    m = ctx.mark()
    parts = []
    for i in range(2):
        info = ctx.fetch(label=f"attempt{i + 1}")
        tsize = temp.stat().st_size if temp.exists() else None
        parts.append(f"#{i + 1}: {summarize(info, 'C3')}, .temp={tsize if tsize is not None else 'none'}")
        if info["rc"] == 0:
            break
    log = [e for e in ctx.log_since(m) if e["kind"] == "blob"]
    ctx.obs["blob_requests"] = log
    return "; ".join(parts) + f"; replies={[(e.get('range'), e['status']) for e in log]}"


# --------------------------------------------------------------------------
# main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", default="2.30,system,2.43,2.47,2.56,openai")
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default=str(HERE / "results"))
    ap.add_argument("--work", default=os.environ.get("S3_WORK", "/tmp/s3work"))
    ap.add_argument("--port", type=int, default=7391)
    a = ap.parse_args()

    work = Path(a.work)
    fx = Fixtures(work / "fx")
    fx.build()
    state_root = work / "state"
    if state_root.exists():
        shutil.rmtree(state_root)
    state_root.mkdir(parents=True)
    (state_root / "packs").symlink_to(fx.packs_dir)
    httpd = server.serve(state_root, fx.backing, a.port)

    clients = [Client(n, work / "homes") for n in a.clients.split(",")]
    only = set(filter(None, a.only.split(",")))
    sids = [s for s in SCENARIOS if not only or s in only]

    results = {"clients": {c.name: c.version for c in clients},
               "fixtures": {"commits": fx.commits,
                            "packs": {k: v for k, v in fx.packs.items() if not k.startswith("many_")}},
               "scenarios": {s: SCENARIOS[s][0] for s in sids},
               "runs": {}}
    for sid in sids:
        title, fn = SCENARIOS[sid]
        for c in clients:
            ctx = Ctx(fx, c, state_root, a.port, sid)
            try:
                summary = fn(ctx)
            except Exception as e:  # a harness bug, not a client result
                summary = f"HARNESS ERROR: {e!r}"
            results["runs"].setdefault(sid, {})[c.name] = {"summary": summary, "obs": ctx.obs}
            print(f"{sid:4} {c.name:7} {summary}", flush=True)
    httpd.shutdown()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps(results, indent=1, default=str))
    lines = ["# S3 packfile-uri matrix (generated by matrix.py)", "",
             "| Client | Version |", "|---|---|"]
    lines += [f"| {k} | `{v}` |" for k, v in results["clients"].items()]
    for sid in sids:
        lines += ["", f"## {sid}: {SCENARIOS[sid][0]}", "", "| Client | Observed |", "|---|---|"]
        for c in clients:
            s = results["runs"][sid][c.name]["summary"].replace("|", "\\|")
            lines.append(f"| {c.name} | {s} |")
    (out / "matrix.md").write_text("\n".join(lines) + "\n")
    print(f"wrote {out / 'matrix.md'}")


if __name__ == "__main__":
    main()
