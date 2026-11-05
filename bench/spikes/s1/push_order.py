#!/usr/bin/env python3
"""Do stock clients ever send a delta before its base in a push pack?

Push path step 2 in docs/PLAN.md stores every pack base-first. Completed
thin bases are written first by Aspen itself, but the received entries are in
whatever order the client chose. This script checks what real clients send.

For every client in the S1 matrix that can push (git 2.30-2.56, JGit,
libgit2, go-git; gitoxide has no push), it pushes deltified history to a
bare repository served by stock git 2.56 `receive-pack` with
`receive.unpackLimit=1`, so the client's pack is kept byte for byte (index-pack
only appends completed thin bases after it). It then parses the kept pack and
counts deltas whose base entry comes *later* in the same pack and was sent by
the client (a "forward base"). OFS_DELTA can't point forward, so only
REF_DELTA can.

Scenarios cross two source layouts with two pushes:
  * loose:  the source repository has only loose objects, so the client
            computes every delta itself;
  * packed: the source was `git gc --aggressive`'d first, so the client may
            reuse on-disk deltas in their on-disk order;
  * full:   push the whole history to an empty repository;
  * incr:   push the second half on top of the first (a thin pack).

    ASPEN_S1_CACHE=... python3 push_order.py      # prints a TSV, exits 1 on a forward base
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import packlib

CACHE = Path(os.environ.get("ASPEN_S1_CACHE", Path.home() / ".cache/aspen-s1"))
GIT_VERSIONS = os.environ.get("GIT_VERSIONS", "2.30.9 2.39.5 2.43.7 2.47.3 2.56.0").split()
SERVER_GIT = CACHE / "git-2.56.0/bin/git"
ENV = {**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "HOME": "", "GIT_AUTHOR_NAME": "s1",
       "GIT_AUTHOR_EMAIL": "s1@example.invalid", "GIT_COMMITTER_NAME": "s1",
       "GIT_COMMITTER_EMAIL": "s1@example.invalid", "GIT_AUTHOR_DATE": "2026-11-03T00:00:00Z",
       "GIT_COMMITTER_DATE": "2026-11-03T00:00:00Z"}


def run(*args, cwd=None, env=None, input=None) -> str:
    r = subprocess.run([str(a) for a in args], cwd=cwd, env=env or ENV, input=input,
                       capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"{args}: {r.stderr.strip()}")
    return r.stdout


def git(*args, cwd=None) -> str:
    return run(SERVER_GIT, *args, cwd=cwd)


def build_source(path: Path, packed: bool) -> list[str]:
    """Return the commit ids of a 24-commit history with plenty of deltas."""
    git("init", "-q", "-b", "main", path)
    files = {f"src/f{i}.txt": [f"file {i} line {j} " + "x" * (j % 37) for j in range(400)]
             for i in range(6)}
    commits = []
    for c in range(24):
        for i, (name, lines) in enumerate(files.items()):
            if (c + i) % 2 == 0:
                lines[(c * 17 + i) % len(lines)] = f"edit {c} in {name}"
                lines.insert((c * 31) % len(lines), f"insert {c}")
        if c % 6 == 5:  # copies and renames give deltas across paths
            files[f"copy/c{c}.txt"] = list(files["src/f0.txt"])
        for name, lines in files.items():
            p = path / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("\n".join(lines) + "\n")
        git("add", "-A", cwd=path)
        git("commit", "-q", "-m", f"commit {c}", cwd=path)
        commits.append(git("rev-parse", "HEAD", cwd=path).strip())
    if packed:
        git("gc", "-q", "--aggressive", cwd=path)
    return commits


def bare(path: Path) -> None:
    git("init", "-q", "--bare", path)
    for k, v in (("receive.unpackLimit", "1"), ("transfer.unpackLimit", "1"),
                 ("http.receivepack", "true"), ("receive.denyNonFastForwards", "false")):
        git("config", k, v, cwd=path)


class Backend(BaseHTTPRequestHandler):
    """Smart HTTP in front of stock `git http-backend` (CGI)."""

    root = ""

    def log_message(self, *a):
        pass

    def _cgi(self, body: bytes) -> None:
        path, _, query = self.path.partition("?")
        env = {**ENV, "GIT_PROJECT_ROOT": self.root, "GIT_HTTP_EXPORT_ALL": "1",
               "REQUEST_METHOD": self.command, "PATH_INFO": path, "QUERY_STRING": query,
               "CONTENT_TYPE": self.headers.get("Content-Type", ""),
               "CONTENT_LENGTH": str(len(body)), "REMOTE_USER": "s1", "REMOTE_ADDR": "127.0.0.1",
               "GIT_PROTOCOL": self.headers.get("Git-Protocol", ""),
               "HTTP_CONTENT_ENCODING": self.headers.get("Content-Encoding", "")}
        exe = CACHE / "git-2.56.0/libexec/git-core/git-http-backend"
        out = subprocess.run([str(exe)], input=body, env=env, capture_output=True).stdout
        head, _, payload = out.partition(b"\r\n\r\n")
        status, headers = 200, []
        for line in head.decode().split("\r\n"):
            k, _, v = line.partition(":")
            if k.lower() == "status":
                status = int(v.split()[0])
            elif k:
                headers.append((k, v.strip()))
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self._cgi(b"")

    def do_POST(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = b""
            while True:
                n = int(self.rfile.readline().strip(), 16)
                if n == 0:
                    self.rfile.readline()
                    break
                body += self.rfile.read(n)
                self.rfile.readline()
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self._cgi(body)


def objects(repo: Path) -> set[str]:
    out = git("cat-file", "--batch-all-objects", "--batch-check=%(objectname)", cwd=repo)
    return set(out.split())


def analyse(repo: Path, before_packs: set[Path], before_objs: set[str]) -> tuple[int, int, int, int]:
    """Return (entries, ofs deltas, ref deltas, forward bases) of the pushed pack."""
    new = [p for p in (repo / "objects/pack").glob("*.pack") if p not in before_packs]
    if len(new) != 1:
        raise RuntimeError(f"expected one kept pack, found {len(new)}")
    pack = new[0]
    _, entries = packlib.parse_pack(pack.read_bytes())
    oid_at = {}
    with open(pack.with_suffix(".idx"), "rb") as f:
        out = subprocess.run([str(SERVER_GIT), "show-index"], stdin=f, capture_output=True,
                             text=True, env=ENV).stdout
    for line in out.splitlines():
        off, oid = line.split()[:2]
        oid_at[int(off)] = oid
    where = {oid: off for off, oid in oid_at.items()}
    ofs = ref = forward = 0
    for e in entries:
        oid = oid_at[e.offset]
        if oid in before_objs:
            continue  # a thin base index-pack appended, not client data
        if e.type == packlib.OBJ_OFS_DELTA:
            ofs += 1
        elif e.type == packlib.OBJ_REF_DELTA:
            ref += 1
            base = e.base_oid.hex()
            if base in where and where[base] > e.offset and base not in before_objs:
                forward += 1
    return len(entries), ofs, ref, forward


def clients(port: int):
    url = f"http://127.0.0.1:{port}/r.git"
    for v in GIT_VERSIONS:
        g = CACHE / f"git-{v}/bin/git"
        rp = CACHE / "git-2.56.0/bin/git-receive-pack"
        yield f"git-{v}", lambda src, dst, g=g, rp=rp: run(
            g, "push", "-q", f"--receive-pack={rp}", f"file://{dst}", "+main:main", cwd=src)
    yield "jgit", lambda src, dst: run(CACHE / "bin/s1-jgit", "push", url, src)
    yield "libgit2", lambda src, dst: run(CACHE / "bin/s1-rust", "git2-push", url, src)
    yield "go-git", lambda src, dst: run(CACHE / "bin/s1-gogit", "push", url, src)


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="s1-push-"))
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    Backend.root = str(work / "srv")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print("client\tsource\tpush\tentries\tofs_delta\tref_delta\tforward_base")
    bad = 0
    try:
        sources = {}
        for layout in ("loose", "packed"):
            sources[layout] = (work / layout, build_source(work / layout, layout == "packed"))
        for name, push in clients(srv.server_address[1]):
            for layout, (src, commits) in sources.items():
                for kind in ("full", "incr"):
                    dst = work / "srv/r.git"
                    shutil.rmtree(dst, ignore_errors=True)
                    bare(dst)
                    if kind == "incr":  # seed the first half with stock git
                        git("push", "-q", f"file://{dst}", f"{commits[11]}:refs/heads/main", cwd=src)
                    before_packs = set((dst / "objects/pack").glob("*.pack"))
                    before_objs = objects(dst)
                    try:
                        push(src, dst)
                        if git("rev-parse", "main", cwd=dst).strip() != commits[-1]:
                            raise RuntimeError("ref not updated")
                        row = analyse(dst, before_packs, before_objs)
                    except Exception as e:  # report and keep going
                        print(f"{name}\t{layout}\t{kind}\terror: {str(e)[-200:]}")
                        bad += 1
                        continue
                    bad += row[3] > 0
                    print(f"{name}\t{layout}\t{kind}\t" + "\t".join(map(str, row)), flush=True)
    finally:
        srv.shutdown()
        shutil.rmtree(work, ignore_errors=True)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
