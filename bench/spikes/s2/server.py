#!/usr/bin/env python3
"""Spike S2: a smart-HTTP Git server that advertises only the trunk branch on push.

This is throwaway spike code, not Aspen. It exists to observe what stock Git
clients do when a receive-pack advertisement hides every branch except trunk,
and to try out the commit-check rules proposed for Aspen's push path.

What it does:

* ``GET  /<repo>.git/info/refs?service=git-receive-pack`` advertises trunk only
  (or nothing for an empty repository). Optional extras, selected per repo with
  ``aspen.*`` keys in the bare repository's git config:

  - ``aspen.haves = N``: also advertise up to N earlier trunk tips as ``.have``
    lines (log-spaced: tip~1, ~2, ~4, ... by reflog entry).
  - ``aspen.explicitHaves = true``: when the client announces ``explicit-haves``
    in its ``Git-Protocol`` header (OpenAI's Git fork does this), advertise every
    ref as a named ref plus the trunk tip as a ``.have`` line, and advertise the
    ``explicit-haves`` capability.
  - ``aspen.adTrunk = <oid>``: advertise this object ID for trunk instead of the
    real tip. Simulates trunk moving between the advertisement (GET) and the
    push (POST) of a stateless HTTP push.
  - ``aspen.noThin = true``: advertise ``no-thin``, so the client sends no
    deltas against objects outside the pack.
  - ``aspen.allowForce = none|branches|all`` (default ``branches``): which refs
    may be force-updated at all. Trunk is never force-updatable under
    ``branches``.

* A scoped URL, ``/<repo>.git/-/<ref-prefix>/...``, advertises trunk plus every
  ref whose name starts with ``<ref-prefix>``. This is the opt-in way for a
  stock client to see (and so delete, lease or fast-forward-check) a branch.

* ``POST /<repo>.git/git-receive-pack`` parses commands, push options and the
  pack; indexes the pack into a quarantine directory (like ``git
  receive-pack``); records what the pack contained; applies the proposed
  Aspen commit-check rules (see ``decide``); and moves refs with a single
  compare-and-swap ``git update-ref --stdin`` transaction per ref (or one for
  the whole push when ``atomic`` was requested).

* Fetches (``git-upload-pack``) are delegated unchanged to ``git http-backend``.
  S2 is about pushes; fetch behaviour is someone else's spike.

Every receive-pack request appends one JSON line to ``<repo>.git/aspen-s2.log``
so the scenario runner can record what the server saw and decided.

Run: ``server.py --root DIR --port PORT``. Repositories are bare repos in DIR.
"""

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

ZERO = "0" * 40
TRUNK = "refs/heads/main"
CAPS = [
    "report-status",
    "delete-refs",
    "side-band-64k",
    "quiet",
    "atomic",
    "ofs-delta",
    "push-options",
    "object-format=sha1",
    "agent=aspen-s2-spike",
]
REPO_LOCKS: dict = {}
REPO_LOCKS_GUARD = threading.Lock()


# ---------------------------------------------------------------- pkt-line


def pkt(data: bytes) -> bytes:
    return b"%04x" % (len(data) + 4) + data


FLUSH = b"0000"


def read_pkts(buf: bytes, pos: int):
    """Yield (payload or None for flush, next_pos) until a flush packet."""
    out = []
    while True:
        n = int(buf[pos : pos + 4], 16)
        if n == 0:
            return out, pos + 4
        out.append(buf[pos + 4 : pos + n])
        pos += n


def sideband(channel: int, data: bytes) -> bytes:
    out = b""
    for i in range(0, len(data), 65515):
        out += pkt(bytes([channel]) + data[i : i + 65515])
    return out


# ---------------------------------------------------------------- git helpers


def git(repo, *args, input=None, env=None, check=True):
    e = dict(os.environ)
    if env:
        e.update(env)
    p = subprocess.run(
        ["git", "--git-dir", repo, *args],
        input=input,
        capture_output=True,
        env=e,
    )
    if check and p.returncode != 0:
        raise RuntimeError(f"git {args} failed: {p.stderr.decode(errors='replace')}")
    return p


def config(repo, key, default=None):
    p = git(repo, "config", "--get", key, check=False)
    return p.stdout.decode().strip() if p.returncode == 0 else default


def all_refs(repo) -> dict:
    out = git(repo, "for-each-ref", "--format=%(objectname) %(refname)").stdout.decode()
    refs = {}
    for line in out.splitlines():
        oid, name = line.split(" ", 1)
        refs[name] = oid
    return refs


def earlier_trunk_tips(repo, n: int) -> list:
    """Up to n earlier trunk tips from the reflog, log-spaced (1, 2, 4, ...)."""
    p = git(repo, "reflog", "show", "--format=%H", TRUNK, "--", check=False)
    tips = []
    for oid in p.stdout.decode().split():
        if oid not in tips:
            tips.append(oid)
    picked, i = [], 1
    while i < len(tips) and len(picked) < n:
        picked.append(tips[i])
        i *= 2
    return picked


def is_ancestor(repo, anc, desc, env=None) -> bool:
    return git(repo, "merge-base", "--is-ancestor", anc, desc, env=env, check=False).returncode == 0


# ---------------------------------------------------------------- advertisement


def advertisement(repo, scope, client_explicit_haves):
    """Return (lines, advertised_names, caps) for a receive-pack advertisement."""
    refs = all_refs(repo)
    caps = list(CAPS)
    if config(repo, "aspen.noThin") == "true":
        caps.append("no-thin")  # clients then send no deltas against objects outside the pack
    named = {}
    haves = []
    explicit = client_explicit_haves and config(repo, "aspen.explicitHaves") == "true"
    if config(repo, "aspen.advertise") == "all":
        named = dict(refs)  # baseline: what stock git-receive-pack advertises
    elif explicit:
        caps.append("explicit-haves")
        named = dict(refs)
        if TRUNK in refs:
            haves.append(refs[TRUNK])
    else:
        if TRUNK in refs:
            named[TRUNK] = config(repo, "aspen.adTrunk") or refs[TRUNK]
        if scope:
            for name, oid in refs.items():
                if name.startswith(scope):
                    named[name] = oid
    nhaves = int(config(repo, "aspen.haves", "0"))
    if nhaves and TRUNK in refs:
        haves += earlier_trunk_tips(repo, nhaves)
    lines = []
    capstr = " ".join(caps).encode()
    items = sorted(named.items())
    if not items:
        lines.append(ZERO.encode() + b" capabilities^{}\0" + capstr + b"\n")
    for i, (name, oid) in enumerate(items):
        line = f"{oid} {name}".encode()
        if i == 0:
            line += b"\0" + capstr
        lines.append(line + b"\n")
    if items:
        for oid in haves:
            lines.append(f"{oid} .have\n".encode())
    return lines, sorted(named), caps


# ---------------------------------------------------------------- commit check


def decide(repo, cmd, current, scope_names, opts, allow_force, qenv):
    """Proposed Aspen commit-check rule for one command.

    Returns (action, reason). action is one of create, update, forced, delete,
    noop, or reject. ``qenv`` lets git see objects that are still in quarantine.
    """
    old, new, ref = cmd["old"], cmd["new"], cmd["ref"]
    advertised = ref == TRUNK or ref in scope_names
    force_opt = "force" in opts or f"force={ref}" in opts
    leases = {o[len("lease="):].rsplit(":", 1)[0]: o.rsplit(":", 1)[1] for o in opts if o.startswith("lease=")}
    if ref in leases:
        # A lease push option replaces the old-oid the client could not know.
        old = leases[ref] if leases[ref] else ZERO
        if (current or ZERO) != old:
            return "reject", f"stale lease (expected {old[:12]}, server has {(current or ZERO)[:12]})"
        force_opt = True  # a matching lease is an explicit, informed overwrite

    def may_force():
        if allow_force == "all":
            return True
        if allow_force == "branches":
            return ref != TRUNK
        return False

    if new == ZERO:
        if current is None:
            return "reject", "no such ref"
        if ref == TRUNK:
            return "reject", "deleting trunk is not allowed"
        if old != ZERO:
            # Scoped URL or lease: the client named the tip it is deleting,
            # so this is a compare-and-swap delete.
            if old != current:
                return "reject", f"stale old-oid (server has {current[:12]})"
            return "delete", "compare-and-swap delete (old-oid matched)"
        # Zero old-oid: the client never saw the ref, so the delete is
        # unconditional. Gate it like a force (PLAN.md decisions log, S2).
        if force_opt:
            if may_force():
                return "delete", "unconditional delete accepted because of -o force"
            return "reject", "unconditional delete needs force, and force is not allowed on this ref"
        return "reject", ("delete of a ref this client has not seen needs -o force, "
                           "-o lease=<ref>:<oid>, or the branch-scoped URL")

    if old != ZERO:
        # The client saw this ref (trunk or a scoped URL) and named its tip.
        if current != old:
            return "reject", f"ref moved (expected {old[:12]}, server has {(current or ZERO)[:12]})"
        if is_ancestor(repo, current, new, qenv):
            return "update", "fast-forward"
        if may_force():
            return "forced", "non-fast-forward with matching old-oid (client forced or leased)"
        return "reject", "non-fast-forward and force is not allowed on this ref"

    # old == ZERO: either a real create, or a ref the client never saw.
    if current is None:
        return "create", ""
    if current == new:
        return "noop", "already at this commit"
    if advertised:
        return "reject", "ref exists (client expected it to be absent)"
    if ref.startswith("refs/tags/"):
        if force_opt and may_force():
            return "forced", "tag replaced (-o force)"
        return "reject", "tag already exists (push with -o force to replace)"
    if is_ancestor(repo, current, new, qenv):
        return "update", "fast-forward (checked server-side; branch was not advertised)"
    if force_opt:
        if may_force():
            return "forced", "non-fast-forward accepted because of -o force"
        return "reject", "non-fast-forward and force is not allowed on this ref"
    return "reject", "non-fast-forward (fetch and rebase, or push with -o force)"


# ---------------------------------------------------------------- receive-pack


def lock_for(repo):
    with REPO_LOCKS_GUARD:
        return REPO_LOCKS.setdefault(repo, threading.Lock())


def receive_pack(repo, scope, body: bytes):
    cmds_raw, pos = read_pkts(body, 0)
    caps = []
    cmds = []
    for i, line in enumerate(cmds_raw):
        if i == 0 and b"\0" in line:
            line, c = line.split(b"\0", 1)
            caps = c.decode().split()
        old, new, ref = line.decode().rstrip("\n").split(" ", 2)
        cmds.append({"old": old, "new": new, "ref": ref})
    opts = []
    if "push-options" in caps:
        raw, pos = read_pkts(body, pos)
        opts = [o.decode().rstrip("\n") for o in raw]
    pack = body[pos:]
    log = {
        "caps": caps,
        "commands": cmds,
        "push_options": opts,
        "pack_bytes": len(pack),
    }
    msgs = []
    statuses = {}
    unpack = "ok"
    qdir = tempfile.mkdtemp(prefix="quarantine-", dir=os.path.join(repo, "objects"))
    qenv = {
        "GIT_OBJECT_DIRECTORY": qdir,
        "GIT_ALTERNATE_OBJECT_DIRECTORIES": os.path.abspath(os.path.join(repo, "objects")),
    }
    try:
        if pack:
            log["pack_header_objects"] = int.from_bytes(pack[8:12], "big") if pack[:4] == b"PACK" else None
            p = git(repo, "index-pack", "--stdin", "--fix-thin", "--strict", input=pack, env=qenv, check=False)
            if p.returncode != 0:
                unpack = "index-pack failed"
                log["index_pack_error"] = p.stderr.decode(errors="replace")
            else:
                idx = [f for f in os.listdir(os.path.join(qdir, "pack")) if f.endswith(".idx")][0]
                with open(os.path.join(qdir, "pack", idx), "rb") as f:
                    shown = subprocess.run(["git", "show-index"], stdin=f, capture_output=True).stdout.decode()
                oids = [l.split()[1] for l in shown.splitlines()]
                log["pack_indexed_objects"] = len(oids)
                log["thin_bases_appended"] = len(oids) - (log["pack_header_objects"] or 0)
                # Objects the server already had (redundant bytes on the wire).
                chk = git(repo, "cat-file", "--batch-check=%(objectname)", input="\n".join(oids).encode() + b"\n", check=False)
                log["objects_already_on_server"] = sum(1 for l in chk.stdout.decode().splitlines() if not l.endswith("missing"))
                # Dependency soundness: objects needed beyond the *current*
                # trunk tip that the pack does not carry. Zero means a reader
                # holding current trunk can use this pack alone.
                trunk_now = all_refs(repo).get(TRUNK)
                packset = set(oids)
                missing = 0
                for c in cmds:
                    if c["new"] != ZERO:
                        args = ["rev-list", "--objects", c["new"]] + (["--not", trunk_now] if trunk_now else [])
                        need = git(repo, *args, env=qenv, check=False).stdout.decode().split("\n")
                        missing += sum(1 for l in need if l and l.split(" ")[0] not in packset)
                log["objects_needed_beyond_trunk_not_in_pack"] = missing
                # Connectivity: every new tip must be fully reachable using
                # quarantine + existing objects (git receive-pack does this too).
                for c in cmds:
                    if c["new"] != ZERO:
                        r = git(repo, "rev-list", "--objects", "--quiet", c["new"], "--not", "--all", env=qenv, check=False)
                        if r.returncode != 0:
                            statuses[c["ref"]] = ("reject", "missing necessary objects")
        atomic = "atomic" in caps
        allow_force = config(repo, "aspen.allowForce", "branches")
        with lock_for(repo):  # stands in for the per-ref CAS; makes the log ordered
            refs = all_refs(repo)
            scope_names = [n for n in refs if scope and n.startswith(scope)]
            decisions = []
            for c in cmds:
                if unpack != "ok":
                    decisions.append(("reject", "unpacker error"))
                elif c["ref"] in statuses:
                    decisions.append(statuses[c["ref"]])
                else:
                    decisions.append(decide(repo, c, refs.get(c["ref"]), scope_names, opts, allow_force, qenv))
            if atomic and any(d[0] == "reject" for d in decisions):
                decisions = [d if d[0] == "reject" else ("reject", "atomic push failed") for d in decisions]
            if any(d[0] != "reject" for d in decisions) and pack and unpack == "ok":
                # Migrate quarantine into the repository (Aspen: publish the pack).
                for f in os.listdir(os.path.join(qdir, "pack")):
                    shutil.move(os.path.join(qdir, "pack", f), os.path.join(repo, "objects", "pack", f))
            # Compare-and-swap ref updates.
            groups = [list(zip(cmds, decisions))] if atomic else [[cd] for cd in zip(cmds, decisions)]
            for group in groups:
                lines = ["start"]
                for c, (action, _) in group:
                    cur = refs.get(c["ref"]) or ZERO
                    if action in ("create", "update", "forced"):
                        lines.append(f"update {c['ref']} {c['new']} {cur}")
                    elif action == "delete":
                        lines.append(f"delete {c['ref']} {cur}")
                lines += ["prepare", "commit"]
                if len(lines) > 3:
                    r = git(repo, "update-ref", "--stdin", input=("\n".join(lines) + "\n").encode(), check=False)
                    if r.returncode != 0:
                        for c, _ in group:
                            decisions[cmds.index(c)] = ("reject", "compare-and-swap failed: " + r.stderr.decode().strip())
            for c, (action, reason) in zip(cmds, decisions):
                c["decision"] = action
                c["reason"] = reason
                if action == "reject":
                    msgs.append(f"aspen: {c['ref']}: rejected: {reason}")
                elif reason:
                    msgs.append(f"aspen: {c['ref']}: {action}: {reason}")
    finally:
        shutil.rmtree(qdir, ignore_errors=True)
    log["unpack"] = unpack
    with open(os.path.join(repo, "aspen-s2.log"), "a") as f:
        f.write(json.dumps(log) + "\n")

    report = pkt(f"unpack {unpack}\n".encode())
    for c in cmds:
        if c.get("decision", "reject") == "reject":
            report += pkt(f"ng {c['ref']} {c.get('reason', 'rejected')}\n".encode())
        else:
            report += pkt(f"ok {c['ref']}\n".encode())
    report += FLUSH
    if "side-band-64k" in caps:
        # Explanations go out even under "quiet": quiet suppresses progress,
        # and clients request it whenever stderr is not a terminal.
        out = b"".join(sideband(2, (m + "\n").encode()) for m in msgs)
        return out + sideband(1, report) + FLUSH
    return report


# ---------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    root = "."
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[s2] " + fmt % args + "\n")

    def split(self):
        """Return (repo_dir, scope, service_path, query) for a request path."""
        u = urlsplit(self.path)
        path = u.path
        i = path.find(".git")
        repo_name = path[1 : i + 4]
        rest = path[i + 4 :]
        scope = None
        if rest.startswith("/-/"):
            rest = rest[3:]
            for suffix in ("/info/refs", "/git-receive-pack", "/git-upload-pack", "/HEAD"):
                if rest.endswith(suffix):
                    scope = rest[: -len(suffix)]
                    rest = suffix
                    break
        return os.path.join(self.root, repo_name), scope, rest, parse_qs(u.query)

    def body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            data = b""
            while True:
                size = int(self.rfile.readline().strip().split(b";")[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                data += self.rfile.read(size)
                self.rfile.readline()
        else:
            data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.headers.get("Content-Encoding") in ("gzip", "x-gzip"):
            data = gzip.decompress(data)
        return data

    def send(self, code, ctype, data):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        repo, scope, rest, q = self.split()
        if not os.path.isdir(repo):
            return self.send(404, "text/plain", b"no such repo\n")
        if rest == "/info/refs" and q.get("service") == ["git-receive-pack"]:
            gp = self.headers.get("Git-Protocol", "")
            lines, names, caps = advertisement(repo, scope, "explicit-haves" in gp.split(":"))
            data = pkt(b"# service=git-receive-pack\n") + FLUSH + b"".join(pkt(l) for l in lines) + FLUSH
            with open(os.path.join(repo, "aspen-s2.log"), "a") as f:
                f.write(json.dumps({"advertised": names, "scope": scope, "git_protocol": gp,
                                    "ad_lines": [l.decode(errors="replace").replace("\0", " \\0 ").rstrip() for l in lines]}) + "\n")
            return self.send(200, "application/x-git-receive-pack-advertisement", data)
        return self.cgi("GET", repo, rest, b"")

    def do_POST(self):
        repo, scope, rest, q = self.split()
        data = self.body()
        if rest == "/git-receive-pack":
            return self.send(200, "application/x-git-receive-pack-result", receive_pack(repo, scope, data))
        return self.cgi("POST", repo, rest, data)

    def cgi(self, method, repo, rest, data):
        """Delegate fetches to git http-backend."""
        env = dict(os.environ)
        env.update({
            "GIT_PROJECT_ROOT": os.path.dirname(os.path.abspath(repo)),
            "GIT_HTTP_EXPORT_ALL": "1",
            "PATH_INFO": "/" + os.path.basename(repo) + rest,
            "REQUEST_METHOD": method,
            "QUERY_STRING": urlsplit(self.path).query,
            "CONTENT_TYPE": self.headers.get("Content-Type", ""),
            "CONTENT_LENGTH": str(len(data)),
            "GIT_PROTOCOL": self.headers.get("Git-Protocol", ""),
            "REMOTE_ADDR": "127.0.0.1",
        })
        p = subprocess.run(["git", "http-backend"], input=data, capture_output=True, env=env)
        head, _, payload = p.stdout.partition(b"\r\n\r\n")
        code, ctype = 200, "application/octet-stream"
        for h in head.split(b"\r\n"):
            k, _, v = h.decode().partition(":")
            if k.lower() == "status":
                code = int(v.strip().split()[0])
            elif k.lower() == "content-type":
                ctype = v.strip()
        return self.send(code, ctype, payload)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", required=True)
    ap.add_argument("--port", type=int, required=True)
    a = ap.parse_args()
    Handler.root = a.root
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
