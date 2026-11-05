#!/usr/bin/env python3
"""Spike S5 matrix: what stock git does with shallow and filter requests.

For each git binary, protocol version (0, 2) and server mode (see capproxy.py),
run every scenario below against a fresh copy of a fixture repository and
record: exit code, the last lines of stderr, whether the result is shallow,
how many commits and objects the client ended up with, whether
`git fsck --strict` passes, and what the client put on the wire.

Usage:
    python3 matrix.py --git git --git /opt/git-2.30.0/bin/git --out results/

Writes results/matrix.json, results/matrix.md and results/transcripts/*.txt.
Only the Python standard library and git are needed.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MODES = ["stock", "strip", "nofilter", "refuse", "overfetch", "planner", "tipsonly"]
PROTOCOLS = [0, 2]
COMMITS = 30


def sh(cmd, cwd=None, env=None, check=True):
    p = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError("%s failed (%d): %s" % (" ".join(cmd), p.returncode, p.stderr))
    return p


def base_env(home):
    env = {
        "PATH": os.environ["PATH"],
        "HOME": home,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "S5 Spike",
        "GIT_AUTHOR_EMAIL": "s5@example.invalid",
        "GIT_COMMITTER_NAME": "S5 Spike",
        "GIT_COMMITTER_EMAIL": "s5@example.invalid",
        "GIT_TERMINAL_PROMPT": "0",
        "LC_ALL": "C",
    }
    return env


def make_fixture(path, env):
    """30 commits on main, one per day from 2024-01-01; an annotated tag v1.0 at
    commit 10; branch topic forked at commit 25. Each commit rewrites data.bin
    with fresh random bytes, so full history is much larger than the tip."""
    work = path + ".work"
    sh(["git", "init", "-q", "-b", "main", work], env=env)
    for i in range(1, COMMITS + 1):
        with open(os.path.join(work, "log.txt"), "a") as f:
            f.write("commit %d\n" % i)
        with open(os.path.join(work, "data.bin"), "wb") as f:
            f.write(os.urandom(16 * 1024))
        with open(os.path.join(work, "file%02d.txt" % (i % 7)), "w") as f:
            f.write(("text for commit %d\n" % i) * 50)
        e = dict(env)
        date = "2024-01-%02dT12:00:00Z" % i
        e["GIT_AUTHOR_DATE"] = date
        e["GIT_COMMITTER_DATE"] = date
        sh(["git", "add", "-A"], cwd=work, env=e)
        sh(["git", "commit", "-qm", "commit %d" % i], cwd=work, env=e)
        if i == 10:
            sh(["git", "tag", "-a", "v1.0", "-m", "v1.0"], cwd=work, env=e)
        if i == 25:
            sh(["git", "branch", "topic"], cwd=work, env=e)
    sh(["git", "checkout", "-q", "topic"], cwd=work, env=env)
    for i in (1, 2):
        with open(os.path.join(work, "topic.txt"), "a") as f:
            f.write("topic %d\n" % i)
        sh(["git", "add", "-A"], cwd=work, env=env)
        sh(["git", "commit", "-qm", "topic %d" % i], cwd=work, env=env)
    sh(["git", "checkout", "-q", "main"], cwd=work, env=env)
    sh(["git", "clone", "-q", "--bare", work, path], env=env)
    shutil.rmtree(work)


def add_server_commit(bare, env, label):
    """Advance main on the server by one commit (simulates another pusher)."""
    work = tempfile.mkdtemp(prefix="s5-push-")
    sh(["git", "clone", "-q", bare, work], env=env)
    with open(os.path.join(work, "log.txt"), "a") as f:
        f.write("server commit %s\n" % label)
    with open(os.path.join(work, "data.bin"), "wb") as f:
        f.write(os.urandom(16 * 1024))
    sh(["git", "add", "-A"], cwd=work, env=env)
    sh(["git", "commit", "-qm", "server commit %s" % label], cwd=work, env=env)
    sh(["git", "push", "-q", "origin", "main"], cwd=work, env=env)
    shutil.rmtree(work)


# --------------------------------------------------------------- scenarios
#
# Each scenario is (id, description, prep, action).
#   prep(ctx)   runs against the *stock* server with system git, to build the
#               client state the action starts from (or None).
#   action(ctx) returns the argv run with the git under test, cwd, and the
#               directory to inspect afterwards.


def sc_clone(extra):
    def action(ctx):
        return ctx.git + ["-c", "protocol.version=%d" % ctx.proto, "clone"] + extra + [ctx.url, "c"], ctx.tmp, os.path.join(ctx.tmp, "c")
    return action


def prep_clone(extra):
    def prep(ctx):
        sh(["git", "clone", "-q"] + extra + [ctx.stock_url, "c"], cwd=ctx.tmp, env=ctx.env)
        sh(["git", "remote", "set-url", "origin", ctx.url], cwd=os.path.join(ctx.tmp, "c"), env=ctx.env)
        add_server_commit(ctx.bare, ctx.env, "after-prep")
    return prep


def prep_resurrect(ctx):
    """Depth-1 clone, then the server re-adds a blob that exists only in history
    older than the client's shallow boundary (file03.txt as of commit 3)."""
    sh(["git", "clone", "-q", "--depth", "1", ctx.stock_url, "c"], cwd=ctx.tmp, env=ctx.env)
    sh(["git", "remote", "set-url", "origin", ctx.url], cwd=os.path.join(ctx.tmp, "c"), env=ctx.env)
    work = tempfile.mkdtemp(prefix="s5-push-")
    sh(["git", "clone", "-q", ctx.bare, work], env=ctx.env)
    with open(os.path.join(work, "file03.txt"), "w") as f:
        f.write(("text for commit %d\n" % 3) * 50)
    sh(["git", "commit", "-qam", "restore file03.txt from commit 3"], cwd=work, env=ctx.env)
    sh(["git", "push", "-q", "origin", "main"], cwd=work, env=ctx.env)
    shutil.rmtree(work)


def act_in_clone(args):
    def action(ctx):
        return ctx.git + ["-c", "protocol.version=%d" % ctx.proto] + args, os.path.join(ctx.tmp, "c"), os.path.join(ctx.tmp, "c")
    return action


def prep_empty_repo(ctx):
    d = os.path.join(ctx.tmp, "c")
    sh(["git", "init", "-q", d], env=ctx.env)
    sh(["git", "remote", "add", "origin", ctx.url], cwd=d, env=ctx.env)


def act_ci_fetch(ctx):
    # What actions/checkout@v4 runs by default (fetch-depth: 1).
    sha = sh(["git", "rev-parse", "refs/heads/main"], cwd=ctx.bare, env=ctx.env).stdout.strip()
    d = os.path.join(ctx.tmp, "c")
    return ctx.git + ["-c", "protocol.version=%d" % ctx.proto, "fetch", "--no-tags", "--prune",
                      "--no-recurse-submodules", "--depth=1", "origin",
                      "+%s:refs/remotes/origin/main" % sha], d, d


def act_gitlab_fetch(ctx):
    # What GitLab Runner runs with its default GIT_DEPTH (20).
    d = os.path.join(ctx.tmp, "c")
    return ctx.git + ["-c", "protocol.version=%d" % ctx.proto, "fetch", "origin",
                      "+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*",
                      "--depth", "20", "--prune", "--quiet"], d, d


def act_lazy_fetch(ctx):
    # A partial clone touching an old commit triggers a lazy blob fetch.
    d = os.path.join(ctx.tmp, "c")
    return ctx.git + ["-c", "protocol.version=%d" % ctx.proto, "checkout", "-q", "HEAD~20"], d, d


SCENARIOS = [
    ("clone-full", "plain clone (baseline)", None, sc_clone([])),
    ("clone-depth1", "clone --depth 1", None, sc_clone(["--depth", "1"])),
    ("clone-since", "clone --shallow-since=2024-01-25", None, sc_clone(["--shallow-since=2024-01-25"])),
    ("clone-exclude", "clone --shallow-exclude=v1.0", None, sc_clone(["--shallow-exclude=v1.0"])),
    ("clone-blobnone", "clone --filter=blob:none", None, sc_clone(["--filter=blob:none"])),
    ("clone-tree0", "clone --filter=tree:0", None, sc_clone(["--filter=tree:0"])),
    ("ci-actions-checkout", "actions/checkout default: fetch --depth=1 <sha>", prep_empty_repo, act_ci_fetch),
    ("ci-gitlab-runner", "GitLab Runner default: fetch --depth 20 refs/heads/*", prep_empty_repo, act_gitlab_fetch),
    ("shallow-fetch", "existing depth-1 clone: git fetch", prep_clone(["--depth", "1"]), act_in_clone(["fetch", "origin"])),
    ("shallow-deepen", "existing depth-1 clone: fetch --deepen=3", prep_clone(["--depth", "1"]), act_in_clone(["fetch", "--deepen=3", "origin"])),
    ("shallow-unshallow", "existing depth-1 clone: fetch --unshallow", prep_clone(["--depth", "1"]), act_in_clone(["fetch", "--unshallow", "origin"])),
    ("shallow-topic", "existing depth-1 clone: fetch topic branch", prep_clone(["--depth", "1"]), act_in_clone(["fetch", "origin", "topic"])),
    ("shallow-resurrect", "existing depth-1 clone: fetch a commit that restores an old blob", prep_resurrect, act_in_clone(["fetch", "origin"])),
    ("partial-fetch", "existing blob:none clone: git fetch", prep_clone(["--filter=blob:none"]), act_in_clone(["fetch", "origin"])),
    ("partial-lazy", "existing blob:none clone: checkout old commit (lazy fetch)", prep_clone(["--filter=blob:none"]), act_lazy_fetch),
]


class Ctx:
    pass


def inspect(repo, env, git):
    info = {}
    if not os.path.isdir(os.path.join(repo, ".git")):
        return info
    def q(args):
        p = subprocess.run(git + args, cwd=repo, env=env, capture_output=True, text=True)
        return p.stdout.strip() if p.returncode == 0 else None
    info["shallow"] = q(["rev-parse", "--is-shallow-repository"]) == "true"
    head = q(["rev-parse", "--verify", "-q", "refs/remotes/origin/main"]) or q(["rev-parse", "--verify", "-q", "HEAD"])
    if head:
        info["commits"] = int(q(["rev-list", "--count", head]) or 0)
    co = q(["count-objects", "-v"]) or ""
    kv = dict(l.split(": ", 1) for l in co.splitlines() if ": " in l)
    info["objects"] = int(kv.get("count", 0)) + int(kv.get("in-pack", 0))
    info["promisor"] = bool(q(["config", "--get", "remote.origin.promisor"]))
    f = subprocess.run(["git", "fsck", "--strict", "--no-progress"], cwd=repo, env=env, capture_output=True, text=True)
    info["fsck"] = f.returncode == 0
    if f.returncode != 0:
        info["fsck_err"] = f.stderr.strip()[-300:]
    return info


def summarize_wire(log_path, start):
    """What the client sent in its upload-pack requests (after the log offset)."""
    sent_deepen, sent_filter, sent_shallow, violations, overfetch, errs = [], [], [], [], [], []
    with open(log_path) as f:
        f.seek(start)
        for line in f:
            e = json.loads(line)
            for r in e.get("request", []):
                if r.startswith(("deepen", "deepen-since", "deepen-not")):
                    sent_deepen.append(r)
                elif r.startswith("filter "):
                    sent_filter.append(r)
                elif r.startswith("shallow "):
                    sent_shallow.append(r[:20])
            violations += e.get("violations", [])
            if "overfetch" in e:
                overfetch.append(e["overfetch"])
            if "planner" in e:
                overfetch.append({"planner": e["planner"]})
            for r in e.get("response", []):
                if r.startswith("ERR"):
                    errs.append(r)
    return {"sent_deepen": sorted(set(sent_deepen)), "sent_filter": sorted(set(sent_filter)),
            "sent_shallow_lines": len(sent_shallow), "violations": sorted(set(violations)),
            "overfetch": overfetch, "server_err": errs}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--git", action="append", required=True, help="git binary under test (repeatable)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--only", help="comma-separated scenario ids")
    args = ap.parse_args()

    out = os.path.abspath(args.out)
    os.makedirs(os.path.join(out, "transcripts"), exist_ok=True)
    work = tempfile.mkdtemp(prefix="s5-matrix-")
    home = os.path.join(work, "home")
    os.makedirs(home)
    env = base_env(home)
    root = os.path.join(work, "root")
    os.makedirs(root)
    fixture = os.path.join(work, "fixture.git")
    make_fixture(fixture, env)

    log_path = os.path.join(out, "requests.jsonl")
    open(log_path, "w").close()
    port_file = os.path.join(work, "port")
    proxy = subprocess.Popen([sys.executable, os.path.join(HERE, "capproxy.py"), "--root", root,
                              "--log", log_path, "--port-file", port_file],
                             stderr=subprocess.DEVNULL)
    for _ in range(100):
        if os.path.exists(port_file) and open(port_file).read():
            break
        time.sleep(0.05)
    port = int(open(port_file).read())

    only = set(args.only.split(",")) if args.only else None
    results = []
    n = 0
    try:
        for gitbin in args.git:
            ver = sh([gitbin, "--version"]).stdout.strip().replace("git version ", "")
            for sid, desc, prep, action in SCENARIOS:
                if only and sid not in only:
                    continue
                for proto in PROTOCOLS:
                    for mode in MODES:
                        n += 1
                        name = "r%04d" % n
                        bare = os.path.join(root, name + ".git")
                        shutil.copytree(fixture, bare)
                        ctx = Ctx()
                        ctx.git, ctx.proto, ctx.env, ctx.bare = [gitbin], proto, env, bare
                        ctx.url = "http://127.0.0.1:%d/%s/%s.git" % (port, mode, name)
                        ctx.stock_url = "http://127.0.0.1:%d/stock/%s.git" % (port, name)
                        ctx.tmp = tempfile.mkdtemp(prefix=name + "-", dir=work)
                        if prep:
                            prep(ctx)
                        start = os.path.getsize(log_path)
                        argv, cwd, inspect_dir = action(ctx)
                        p = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=120)
                        wire = summarize_wire(log_path, start)
                        info = inspect(inspect_dir, env, [gitbin])
                        rec = {"git": ver, "scenario": sid, "desc": desc, "protocol": proto, "mode": mode,
                               "exit": p.returncode, "stderr": p.stderr.strip().splitlines()[-4:],
                               **info, **wire}
                        if mode == "overfetch" and proto == 0:
                            rec["note"] = "overfetch not modelled for v0; behaves like nofilter"
                        results.append(rec)
                        tname = "%s__git-%s__v%d__%s.txt" % (sid, ver, proto, mode)
                        with open(os.path.join(out, "transcripts", tname), "w") as t:
                            t.write("$ %s\n(cwd: %s)\nexit %d\n\n--- stderr ---\n%s\n--- result ---\n%s\n" % (
                                " ".join(argv).replace(work, "$WORK"), cwd.replace(work, "$WORK"), p.returncode,
                                p.stderr.replace(work, "$WORK"), json.dumps({**info, **wire}, indent=2)))
                        shutil.rmtree(ctx.tmp, ignore_errors=True)
                        shutil.rmtree(bare, ignore_errors=True)
                        sys.stderr.write("%s %s v%d %-9s exit=%d\n" % (ver, sid, proto, mode, p.returncode))
    finally:
        proxy.terminate()
        shutil.rmtree(work, ignore_errors=True)

    with open(os.path.join(out, "matrix.json"), "w") as f:
        json.dump(results, f, indent=1)
    write_markdown(results, os.path.join(out, "matrix.md"))


def cell(r):
    if r["exit"] != 0:
        msg = next((l for l in r["stderr"] if l.startswith("fatal:")), r["stderr"][-1] if r["stderr"] else "")
        return "FAIL: " + msg.replace("|", "\\|")[:140]
    bits = ["ok"]
    if r.get("shallow"):
        bits.append("shallow")
    if r.get("promisor"):
        bits.append("partial")
    if "commits" in r:
        bits.append("%d commits" % r["commits"])
    if "objects" in r:
        bits.append("%d objs" % r["objects"])
    if r.get("fsck") is False:
        bits.append("FSCK FAIL")
    warn = [l for l in r["stderr"] if l.startswith("warning:")]
    if warn:
        bits.append("(" + warn[0].replace("|", "\\|")[:80] + ")")
    if r.get("violations"):
        bits.append("VIOLATION: " + "; ".join(r["violations"]))
    for o in r.get("overfetch", []):
        if "full_pack_bytes" in o:
            bits.append("pack %dB vs exact %dB" % (o["full_pack_bytes"], o["exact_shallow_pack_bytes"]))
    return ", ".join(bits)


def write_markdown(results, path):
    gits = sorted({r["git"] for r in results}, key=lambda v: [int(x) for x in v.split(".")[:3]])
    lines = ["# S5 matrix results", "",
             "Generated by `matrix.py`. Each cell: outcome, then the client's resulting state.",
             "Modes are described in `capproxy.py`. `objs` counts objects in the client repository.", ""]
    for g in gits:
        lines += ["## git %s" % g, ""]
        for proto in PROTOCOLS:
            lines += ["### protocol v%d" % proto, "", "| scenario | " + " | ".join(MODES) + " |",
                      "|---|" + "---|" * len(MODES)]
            for sid, desc, _, _ in SCENARIOS:
                row = [r for r in results if r["git"] == g and r["protocol"] == proto and r["scenario"] == sid]
                if not row:
                    continue
                by = {r["mode"]: r for r in row}
                lines.append("| %s | %s |" % (desc, " | ".join(cell(by[m]) if m in by else "" for m in MODES)))
            lines.append("")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
