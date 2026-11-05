#!/usr/bin/env python3
"""Spike S2 scenario runner.

Runs every push scenario against ``server.py`` with each Git client given on
the command line, and writes one transcript per (client, scenario) plus a
summary matrix to ``--out``.

    ./run.py --out results git-2.39=/usr/bin/git git-2.30=clients/2.30.9/bin/git ...

Only ``results/matrix.md`` is committed. The per-client transcripts
(``results/<client>/<scenario>.md``, about 120 files) are gitignored; this
command regenerates them in about two minutes. Decisive excerpts are quoted
in ``wire-examples.md``.

Each scenario starts from a fresh server repository whose trunk
(``refs/heads/main``) has three commits, and a fresh client clone. Commit
dates are fixed, so object IDs are identical across runs and clients.
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ZERO = "0" * 40


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class World:
    """One server repo + one client clone, driven by one client binary."""

    def __init__(self, base, port, client_git, name, repo_config):
        self.base, self.port, self.cg, self.name = base, port, client_git, name
        self.srv = os.path.join(base, "srv", name + ".git")
        self.cli = os.path.join(base, "cli", name)
        self.other = os.path.join(base, "other", name)  # a second, "other agent" clone
        self.home = os.path.join(base, "home")
        self.tick = 0
        self.steps = []
        self.trace = os.path.join(base, "trace", name + ".trace")
        os.makedirs(os.path.dirname(self.trace), exist_ok=True)
        os.makedirs(self.home, exist_ok=True)
        open(os.path.join(self.home, ".gitconfig"), "w").close()
        # Server repo, seeded with the system git through the filesystem.
        sysgit(["init", "-q", "--bare", "-b", "main", self.srv])
        for k, v in {"core.logAllRefUpdates": "always", "http.receivepack": "true", **repo_config}.items():
            sysgit(["--git-dir", self.srv, "config", k, v])
        sysgit(["init", "-q", "-b", "main", self.other])
        for i in range(3):
            self.commit_in(self.other, f"trunk {i}", f"src/file{i}.txt", lines=200)
        sysgit(["-C", self.other, "push", "-q", self.srv, "main"], env=self.env())
        self.url = f"http://127.0.0.1:{port}/{name}.git"
        self.client(["clone", "-q", self.url, self.cli], cwd=base, record=False)
        sysgit(["-C", self.other, "remote", "add", "origin", self.srv])

    # -- environment ---------------------------------------------------------
    def env(self, trace=False):
        self.tick += 1
        e = {
            "HOME": self.home,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "S2 Spike",
            "GIT_AUTHOR_EMAIL": "s2@example.invalid",
            "GIT_COMMITTER_NAME": "S2 Spike",
            "GIT_COMMITTER_EMAIL": "s2@example.invalid",
            "GIT_AUTHOR_DATE": f"2026-11-03T00:00:{self.tick % 60:02d}Z",
            "GIT_COMMITTER_DATE": f"2026-11-03T00:00:{self.tick % 60:02d}Z",
            "PATH": os.environ["PATH"],
            "LANG": "C",
            "LC_ALL": "C",
        }
        if trace:
            e["GIT_TRACE_PACKET"] = self.trace
        return e

    # -- helpers -------------------------------------------------------------
    def commit_in(self, repo, msg, path=None, lines=20):
        path = path or f"notes/{msg.replace(' ', '_')}.txt"
        full = os.path.join(repo, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "a") as f:
            for i in range(lines):
                f.write(f"{msg} line {i}\n")
        sysgit(["-C", repo, "add", path], env=self.env())
        sysgit(["-C", repo, "commit", "-q", "-m", msg], env=self.env())
        return sysgit(["-C", repo, "rev-parse", "HEAD"]).strip()

    def commit(self, msg, path=None, lines=20):
        return self.commit_in(self.cli, msg, path, lines)

    def sh(self, args, repo=None):
        """Run system git in the client clone (setup steps, not recorded)."""
        return sysgit(["-C", repo or self.cli, *args], env=self.env())

    def server_set(self, ref, oid):
        """Move a server ref directly, as if another agent had pushed it."""
        sysgit(["--git-dir", self.srv, "update-ref", ref, oid])

    def server_ref(self, ref):
        p = subprocess.run(["git", "--git-dir", self.srv, "rev-parse", "-q", "--verify", ref], capture_output=True, text=True)
        return p.stdout.strip() or None

    def other_push(self, branch, commits, base="main"):
        """Another agent creates `branch` from `base` with N commits and puts it on the server."""
        self.sh(["fetch", "-q", "origin"], repo=self.other)
        self.sh(["checkout", "-q", "-B", branch, f"origin/{base}" if base != "main" else "main"], repo=self.other)
        for i in range(commits):
            self.commit_in(self.other, f"other {branch} {i}")
        tip = self.sh(["rev-parse", "HEAD"], repo=self.other).strip()
        sysgit(["-C", self.other, "push", "-q", "-f", self.srv, f"HEAD:refs/heads/{branch}"], env=self.env())
        return tip

    def client(self, args, cwd=None, record=True, note=""):
        """Run the client git under test, recording command, output and exit code."""
        if os.path.exists(self.trace):
            os.remove(self.trace)
        logf = os.path.join(self.srv, "aspen-s2.log")
        before = os.path.getsize(logf) if os.path.exists(logf) else 0
        p = subprocess.run([self.cg, *args], cwd=cwd or self.cli, capture_output=True, text=True, env=self.env(trace=record))
        if not record:
            if p.returncode:
                raise RuntimeError(p.stderr)
            return p
        server = []
        if os.path.exists(logf):
            with open(logf) as f:
                f.seek(before)
                server = [json.loads(l) for l in f if l.strip()]
        trace = []
        if os.path.exists(self.trace):
            with open(self.trace, errors="replace") as f:
                for l in f:
                    if "pkt-line.c" in l or "packet:" in l:
                        l = l.split("packet:", 1)[-1].strip()
                        if "PACK" in l and len(l) > 120:
                            l = l[:120] + "..."
                        trace.append(l)
        self.steps.append({"cmd": "git " + " ".join(args), "note": note, "exit": p.returncode,
                           "stdout": p.stdout, "stderr": p.stderr, "server": server, "trace": trace})
        return p


def sysgit(args, env=None):
    e = dict(os.environ)
    e.update(env or {})
    p = subprocess.run(["git", *args], capture_output=True, text=True, env=e)
    if p.returncode:
        raise RuntimeError(f"git {args}: {p.stderr}")
    return p.stdout


# ------------------------------------------------------------------- scenarios
# Each scenario: (id, title, repo_config, fn(w) -> dict of named observations)

def s_create(w):
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1")
    w.client(["push", "origin", "feature"])


def s_ff_update(w):
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1")
    w.client(["push", "origin", "feature"], note="first push (create)")
    w.commit("feature 2")
    w.client(["push", "origin", "feature"], note="second push: fast-forward of a branch the server did not advertise")


def s_noop(w):
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1")
    w.client(["push", "origin", "feature"], note="first push")
    w.client(["push", "origin", "feature"], note="same push again; stock servers say 'Everything up-to-date'")


def _diverged(w):
    """Server has feature at X (another agent); client has feature at Y, both off main."""
    w.other_push("feature", 1)
    w.sh(["fetch", "-q", "origin"])
    w.sh(["checkout", "-q", "-b", "feature", "main"])
    w.commit("client feature 1")


def s_nonff(w):
    _diverged(w)
    w.client(["push", "origin", "feature"], note="non-fast-forward, no force")


def s_force_plain(w):
    _diverged(w)
    w.client(["push", "--force", "origin", "feature"], note="--force alone: the server cannot see the '+'")


def s_force_opt(w):
    _diverged(w)
    w.client(["push", "--force", "-o", "force", "origin", "feature"], note="--force plus the 'force' push option")


def s_force_opt_only(w):
    _diverged(w)
    w.client(["push", "-o", "force", "origin", "feature"], note="'force' push option without --force")


def s_delete(w):
    w.other_push("feature", 1)
    w.sh(["fetch", "-q", "origin"])
    w.client(["push", "origin", "--delete", "feature"], note="delete a branch the server did not advertise")


def s_delete_colon(w):
    w.other_push("feature", 1)
    w.client(["push", "origin", ":refs/heads/feature"],
             note="full ref name, no force: arrives with a zero old-oid, so it is unconditional and gated")
    w.client(["push", "-o", "force", "origin", ":refs/heads/feature"], note="full ref name with -o force")
    x = w.other_push("feature", 1)
    w.client(["push", "-o", f"lease=refs/heads/feature:{x}", "origin", "--delete", "refs/heads/feature"],
             note="--delete full ref name with a matching lease (compare-and-swap delete)")
    w.client(["push", "-o", "force", "origin", "--delete", "refs/heads/feature"],
             note="--delete a full ref name that no longer exists")


def _lease_setup(w):
    x = w.other_push("feature", 1)
    w.sh(["fetch", "-q", "origin"])  # origin/feature = X (fetch sees every ref)
    w.sh(["checkout", "-q", "-b", "feature", "origin/feature"])
    w.sh(["commit", "-q", "--amend", "-m", "client rewrote feature"])
    return x


def s_lease_explicit(w):
    x = _lease_setup(w)
    w.client(["push", f"--force-with-lease=feature:{x}", "origin", "feature"], note="lease with the correct expected value")


def s_lease_implicit(w):
    _lease_setup(w)
    w.client(["push", "--force-with-lease", "origin", "feature"], note="lease against the remote-tracking ref")


def s_lease_opt(w):
    x = _lease_setup(w)
    w.client(["push", "-o", f"lease=refs/heads/feature:{x}", "origin", "feature"], note="lease as a push option (correct value)")


def s_lease_opt_stale(w):
    _lease_setup(w)
    w.other_push("feature", 1)  # someone moves feature after our fetch
    stale = w.sh(["rev-parse", "origin/feature"]).strip()
    w.client(["push", "-o", f"lease=refs/heads/feature:{stale}", "origin", "feature"], note="lease as a push option (stale value)")


def s_atomic_ok(w):
    w.sh(["checkout", "-q", "-b", "a"])
    w.commit("a 1")
    w.sh(["checkout", "-q", "-b", "b", "main"])
    w.commit("b 1")
    w.client(["push", "--atomic", "origin", "a", "b"], note="two new branches, atomic")


def s_atomic_partial(w):
    w.other_push("b", 1)
    w.sh(["checkout", "-q", "-b", "a"])
    w.commit("a 1")
    w.sh(["checkout", "-q", "-b", "b", "main"])
    w.commit("b 1 (diverges from server b)")
    w.client(["push", "--atomic", "origin", "a", "b"], note="a is new, b is non-fast-forward; atomic")
    w.client(["push", "origin", "a", "b"], note="same, not atomic")


def s_atomic_delete(w):
    w.other_push("old", 1)
    w.sh(["checkout", "-q", "-b", "a"])
    w.commit("a 1")
    w.client(["push", "--atomic", "origin", "a", ":refs/heads/old"],
             note="atomic create + unconditional delete without force: the delete fails, so both fail")
    w.client(["push", "--atomic", "-o", "force", "origin", "a", ":refs/heads/old"],
             note="same with -o force")


def s_trunk(w):
    w.commit("trunk change")
    w.client(["push", "origin", "main"], note="fast-forward trunk")
    w.sh(["reset", "-q", "--hard", "HEAD~1"])
    w.commit("diverging trunk change")
    w.client(["push", "origin", "main"], note="non-fast-forward trunk, no force (trunk is advertised)")
    w.client(["push", "--force", "origin", "main"], note="--force on trunk (policy: no force on trunk)")


def s_tags(w):
    w.sh(["tag", "v1"])
    w.client(["push", "origin", "v1"], note="create lightweight tag")
    w.client(["push", "origin", "v1"], note="push the same tag again")
    w.sh(["tag", "-a", "-m", "annotated", "v2"])
    w.client(["push", "--tags", "origin"], note="push --tags (v1 again, v2 new)")
    w.commit("move")
    w.sh(["tag", "-f", "v1"])
    w.client(["push", "origin", "v1"], note="move tag, no force")
    w.client(["push", "--force", "origin", "v1"], note="move tag, --force only")
    w.client(["push", "-o", "force", "origin", "v1"], note="move tag, -o force")


def s_stale_client(w):
    for i in range(20):
        w.commit_in(w.other, f"trunk advance {i}", f"src/adv{i}.txt", lines=200)
    sysgit(["-C", w.other, "push", "-q", w.srv, "main"], env=w.env())
    # The client has NOT fetched: it lacks the advertised trunk tip.
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1")
    w.client(["push", "origin", "feature"], note="client behind trunk by 20 commits creates a branch")


def s_stacked(w):
    w.sh(["checkout", "-q", "-b", "a"])
    for i in range(3):
        w.commit(f"a {i}")
    w.client(["push", "origin", "a"], note="push base branch a (3 commits)")
    w.sh(["checkout", "-q", "-b", "b"])
    w.commit("b 0")
    w.client(["push", "origin", "b"], note="push b stacked on a (1 new commit)")
    w.commit("b 1")
    w.client(["push", "origin", "b"], note="push b again (1 more commit)")


def s_empty_repo(w):
    sysgit(["--git-dir", w.srv, "update-ref", "-d", "refs/heads/main"])
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1")
    w.client(["push", "origin", "feature"], note="first push to an empty repo is a non-trunk branch")
    w.client(["push", "origin", "main"], note="then trunk")


def s_scoped(w):
    x = w.other_push("feature", 1)
    w.sh(["fetch", "-q", "origin"])
    scoped = w.url + "/-/refs/heads/feature"
    w.sh(["checkout", "-q", "-b", "feature", "origin/feature"])
    w.sh(["commit", "-q", "--amend", "-m", "client rewrote feature"])
    w.client(["push", scoped, "feature"], note="scoped URL: non-ff without force (client sees the branch)")
    w.client(["push", f"--force-with-lease=feature:{x}", scoped, "feature"], note="scoped URL: --force-with-lease, correct value")
    w.sh(["commit", "-q", "--amend", "-m", "client rewrote feature again"])
    w.client(["push", f"--force-with-lease=feature:{x}", scoped, "feature"], note="scoped URL: same lease again, now stale")
    w.client(["push", scoped, "--delete", "feature"], note="scoped URL: delete")


def s_explicit_haves(w):
    x = _lease_setup(w)
    w.client(["push", f"--force-with-lease=feature:{x}", "origin", "feature"], note="lease; server offers explicit-haves")
    w.sh(["checkout", "-q", "-b", "c", "main"])
    w.commit("c 1")
    w.client(["push", "origin", "c"], note="new branch; pack should still be built against trunk only")
    w.client(["push", "origin", "--delete", "c"], note="delete via the normal URL")


def s_race_forward(w):
    # Trunk moves on after the client read the advertisement: emulate by
    # advertising trunk~1 while the server's trunk is at trunk.
    w.sh(["fetch", "-q", "origin"])
    tip1 = w.server_ref("refs/heads/main~1")
    sysgit(["--git-dir", w.srv, "config", "aspen.adTrunk", tip1])
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1")
    w.client(["push", "origin", "feature"], note="advertised trunk is one behind the real trunk")


def s_race_rewind(w):
    # Trunk is rewound (force-pushed) after the advertisement: advertise the
    # old tip, which is no longer in trunk's history.
    old_tip = w.server_ref("refs/heads/main")
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("feature 1", path="src/file2.txt")  # touches a file changed by the rewound commit
    w.server_set("refs/heads/main", w.server_ref("refs/heads/main~1"))
    sysgit(["--git-dir", w.srv, "config", "aspen.adTrunk", old_tip])
    w.client(["push", "origin", "feature"], note="advertised trunk was rewound away before the push landed")


def s_thin(w):
    w.sh(["checkout", "-q", "-b", "feature"])
    w.commit("edit a big trunk file", path="src/file0.txt", lines=5)
    w.client(["push", "origin", "feature"], note="branch edits a 200-line trunk file")


def s_prune(w):
    w.other_push("remote-only", 1)
    w.sh(["checkout", "-q", "-b", "mine"])
    w.commit("mine 1")
    w.client(["push", "--prune", "origin", "refs/heads/*:refs/heads/*"], note="--prune cannot see remote-only branches")
    w.client(["push", "--mirror", "origin"], note="--mirror")


TRUNK_ONLY = {}
SCENARIOS = [
    ("01-create", "Create a new branch", TRUNK_ONLY, s_create),
    ("02-ff-update", "Fast-forward an existing, unadvertised branch", TRUNK_ONLY, s_ff_update),
    ("03-noop", "Re-push an unchanged branch", TRUNK_ONLY, s_noop),
    ("04-nonff", "Non-fast-forward push, no force", TRUNK_ONLY, s_nonff),
    ("05-force-plain", "git push --force (non-fast-forward)", TRUNK_ONLY, s_force_plain),
    ("06-force-opt", "git push --force -o force", TRUNK_ONLY, s_force_opt),
    ("07-force-opt-only", "git push -o force (no --force)", TRUNK_ONLY, s_force_opt_only),
    ("08-delete", "git push --delete <unadvertised branch>", TRUNK_ONLY, s_delete),
    ("09-delete-full-name", "Delete by full ref name", TRUNK_ONLY, s_delete_colon),
    ("10-lease-explicit", "--force-with-lease=<ref>:<expect>", TRUNK_ONLY, s_lease_explicit),
    ("11-lease-implicit", "--force-with-lease (remote-tracking)", TRUNK_ONLY, s_lease_implicit),
    ("12-lease-opt", "-o lease=<ref>:<expect>", TRUNK_ONLY, s_lease_opt),
    ("13-lease-opt-stale", "-o lease=<ref>:<stale>", TRUNK_ONLY, s_lease_opt_stale),
    ("14-atomic-ok", "--atomic, all accepted", TRUNK_ONLY, s_atomic_ok),
    ("15-atomic-partial", "--atomic with one non-fast-forward", TRUNK_ONLY, s_atomic_partial),
    ("16-atomic-delete", "--atomic create + delete unadvertised", TRUNK_ONLY, s_atomic_delete),
    ("17-trunk", "Trunk: ff, non-ff, --force", TRUNK_ONLY, s_trunk),
    ("18-tags", "Tags: create, re-push, --tags, move", TRUNK_ONLY, s_tags),
    ("19-stale-client", "Client behind trunk creates a branch", TRUNK_ONLY, s_stale_client),
    ("20-stale-client-haves", "Same, server adds 8 log-spaced .have trunk tips", {"aspen.haves": "8"}, s_stale_client),
    ("21-stacked", "Branch stacked on a branch", TRUNK_ONLY, s_stacked),
    ("22-stacked-full", "Same, baseline full advertisement", {"aspen.advertise": "all"}, s_stacked),
    ("23-empty-repo", "First push to an empty repository", TRUNK_ONLY, s_empty_repo),
    ("24-scoped-url", "Scoped URL /-/<prefix>: lease, delete", TRUNK_ONLY, s_scoped),
    ("25-explicit-haves", "explicit-haves (OpenAI Git fork)", {"aspen.explicitHaves": "true"}, s_explicit_haves),
    ("26-race-forward", "Trunk advanced between advertisement and push", TRUNK_ONLY, s_race_forward),
    ("27-race-rewind", "Trunk rewound between advertisement and push", {"aspen.allowForce": "all"}, s_race_rewind),
    ("28-prune-mirror", "--prune and --mirror", TRUNK_ONLY, s_prune),
    ("29-thin", "Branch edits a large trunk file (thin pack)", TRUNK_ONLY, s_thin),
    ("30-no-thin", "Same, server advertises no-thin", {"aspen.noThin": "true"}, s_thin),
]


# ------------------------------------------------------------------- output

def outcome(step):
    """Classify a push step: client-refused, server-rejected, accepted, mixed."""
    srv = [s for s in step["server"] if "commands" in s]
    if not srv:
        if step["exit"] == 0:
            return "no-op (nothing sent)"
        return "CLIENT REFUSED"
    decisions = [c.get("decision") for s in srv for c in s["commands"]]
    if all(d == "reject" for d in decisions):
        return "SERVER REJECTED"
    if all(d != "reject" for d in decisions):
        return "accepted (" + ",".join(decisions) + ")"
    return "mixed (" + ",".join(decisions) + ")"


def pack_summary(step):
    for s in step["server"]:
        if "commands" in s and s.get("pack_bytes"):
            return (f"{s.get('pack_header_objects')} obj / {s['pack_bytes']} B, "
                    f"thin bases {s.get('thin_bases_appended')}, already on server {s.get('objects_already_on_server')}, "
                    f"missing vs trunk {s.get('objects_needed_beyond_trunk_not_in_pack')}")
    return ""


def write_transcript(path, client, sid, title, w):
    with open(path, "w") as f:
        f.write(f"# {sid}: {title}\n\nClient: `{client}`\n\n")
        for i, st in enumerate(w.steps, 1):
            f.write(f"## Step {i}: `{st['cmd']}`\n\n")
            if st["note"]:
                f.write(f"{st['note']}\n\n")
            f.write(f"Outcome: **{outcome(st)}** (exit {st['exit']})")
            ps = pack_summary(st)
            f.write(f"; pack: {ps}\n\n" if ps else "\n\n")
            f.write("Client output:\n\n```text\n" + (st["stdout"] + st["stderr"]).strip() + "\n```\n\n")
            for s in st["server"]:
                if "advertised" in s:
                    f.write("Server advertisement (GET info/refs):\n\n```text\n" + "\n".join(s["ad_lines"]) +
                            (f"\n[Git-Protocol: {s['git_protocol']}]" if s["git_protocol"] else "") + "\n```\n\n")
                else:
                    f.write("Server saw and decided (POST git-receive-pack):\n\n```json\n" + json.dumps(s, indent=1) + "\n```\n\n")
            if st["trace"]:
                t = st["trace"]
                if len(t) > 40:
                    t = t[:40] + [f"... {len(st['trace']) - 40} more lines"]
                f.write("Client packet trace (GIT_TRACE_PACKET):\n\n```text\n" + "\n".join(t) + "\n```\n\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--only", help="comma-separated scenario ids")
    ap.add_argument("clients", nargs="+", help="label=/path/to/git")
    a = ap.parse_args()
    clients = [c.split("=", 1) for c in a.clients]
    base = tempfile.mkdtemp(prefix="aspen-s2-")
    os.makedirs(os.path.join(base, "srv"))
    port = free_port()
    server = subprocess.Popen([sys.executable, os.path.join(HERE, "server.py"), "--root", os.path.join(base, "srv"),
                               "--port", str(port)], stderr=subprocess.DEVNULL)
    time.sleep(0.5)
    matrix = {}
    try:
        os.makedirs(a.out, exist_ok=True)
        for label, path in clients:
            ver = subprocess.run([path, "--version"], capture_output=True, text=True).stdout.strip()
            cdir = os.path.join(a.out, label)
            os.makedirs(cdir, exist_ok=True)
            for sid, title, cfg, fn in SCENARIOS:
                if a.only and sid not in a.only.split(","):
                    continue
                w = World(base, port, path, f"{label}-{sid}", cfg)
                try:
                    fn(w)
                except Exception as e:  # record harness failures instead of hiding them
                    w.steps.append({"cmd": "(harness)", "note": f"harness error: {e}", "exit": -1,
                                    "stdout": "", "stderr": "", "server": [], "trace": []})
                write_transcript(os.path.join(cdir, sid + ".md"), ver, sid, title, w)
                matrix[(sid, label)] = [(st["note"] or st["cmd"], outcome(st), pack_summary(st)) for st in w.steps]
                print(f"{label:8} {sid:24} " + " | ".join(o for _, o, _ in matrix[(sid, label)]), flush=True)
    finally:
        server.terminate()
    with open(os.path.join(a.out, "matrix.md"), "w") as f:
        f.write("# S2 outcome matrix\n\nGenerated by `run.py`. One row per push step; one column per client.\n\n")
        labels = [l for l, _ in clients]
        f.write("| Scenario | Step | " + " | ".join(labels) + " |\n|---|---|" + "---|" * len(labels) + "\n")
        for sid, title, _, _ in SCENARIOS:
            rows = matrix.get((sid, labels[0]))
            if rows is None:
                continue
            for i, (step, _, _) in enumerate(rows):
                cells = []
                for l in labels:
                    r = matrix.get((sid, l), [])
                    cells.append(r[i][1] if i < len(r) else "?")
                f.write(f"| {sid} {title if i == 0 else ''} | {step} | " + " | ".join(cells) + " |\n")
        f.write("\n## Pack contents (first client)\n\n| Scenario | Step | Pack |\n|---|---|---|\n")
        for sid, title, _, _ in SCENARIOS:
            for step, _, ps in matrix.get((sid, labels[0]), []):
                if ps:
                    f.write(f"| {sid} | {step} | {ps} |\n")
    shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
