#!/usr/bin/env python3
"""Run the S1 matrix: every client configuration against every scenario.

    ./setup.sh             # once: builds/downloads the clients
    ./run_matrix.py        # writes results.tsv and MATRIX.md next to this file

Options: --only-clients REGEX, --only-scenarios REGEX, --work DIR.

For each (client configuration, scenario) the runner:
  1. creates a fresh repository with git 2.30 (oldest format every client reads),
     and installs the scenario's seed pack if the client is meant to have history;
  2. has the client fetch +refs/heads/main:refs/remotes/origin/main from the
     fake server, which answers with the scenario's stitched pack;
  3. verifies the result with the newest git: refs/remotes/origin/main is the
     expected tip, `git fsck --strict --full` passes, and the expected number of
     objects is reachable. It also counts duplicate object ids in the client's
     .idx files, i.e. whether duplicates were written to disk.

Outcome per cell: ACCEPT (client succeeded and the repository verifies),
REJECT (client exited non-zero), CORRUPT (client succeeded but the repository
does not verify), TIMEOUT.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import resource
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fake_server  # noqa: E402

CACHE = os.environ.get("ASPEN_S1_CACHE", os.path.expanduser("~/.cache/aspen-s1"))
GIT_VERSIONS = os.environ.get("GIT_VERSIONS", "2.30.9 2.39.5 2.43.7 2.47.3 2.56.0").split()
REF_GIT = os.path.join(CACHE, f"git-{GIT_VERSIONS[-1]}", "bin", "git")
INIT_GIT = os.path.join(CACHE, f"git-{GIT_VERSIONS[0]}", "bin", "git")
TIMEOUT = 120


def git_env(home: str) -> dict:
    return dict(os.environ, HOME=home, GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0")


def client_configs() -> list[dict]:
    """Each config: id, client, version, protocol, variant, run(scenario_dir, url, repo) -> argv."""
    cfgs = []
    for v in GIT_VERSIONS:
        gitbin = os.path.join(CACHE, f"git-{v}", "bin", "git")
        for proto in ("0", "2"):
            for path, limit in (("index-pack", "1"), ("unpack-objects", "100000")):
                for fsck in ("false", "true"):
                    cfgs.append({
                        "id": f"git-{v} v{proto} {path}{' fsck' if fsck == 'true' else ''}",
                        "client": "git", "version": v, "protocol": f"v{proto}",
                        "variant": path + (" +fsckObjects" if fsck == "true" else ""),
                        "transport": "pipe",
                        "argv": lambda sd, url, repo, gitbin=gitbin, proto=proto, limit=limit, fsck=fsck: [
                            gitbin, "-c", f"protocol.version={proto}", "-c", f"fetch.unpackLimit={limit}",
                            "-c", f"fetch.fsckObjects={fsck}", "-c", "gc.auto=0", "-c", "maintenance.auto=false",
                            "fetch", "--no-tags", f"--upload-pack=python3 {os.path.join(HERE, 'fake_server.py')} upload-pack",
                            "file://" + sd, "+refs/heads/main:refs/remotes/origin/main"],
                        "repo_config": {},
                    })
    jgit_ver = _jgit_version()
    for proto in ("0", "2"):
        for fsck in ("false", "true"):
            cfgs.append({
                "id": f"jgit v{proto}{' fsck' if fsck == 'true' else ''}", "client": "jgit", "version": jgit_ver,
                "protocol": f"v{proto}", "variant": "+fsckObjects" if fsck == "true" else "default",
                "transport": "http",
                "argv": lambda sd, url, repo: [os.path.join(CACHE, "bin", "s1-jgit"), url, repo],
                "repo_config": {"protocol.version": proto, "fetch.fsckObjects": fsck},
            })
    gix_ver, git2_ver = _cargo_versions()
    for proto in ("1", "2"):
        cfgs.append({
            "id": f"gitoxide v{proto}", "client": "gitoxide", "version": gix_ver, "protocol": f"v{proto}",
            "variant": "default", "transport": "http",
            "argv": lambda sd, url, repo: [os.path.join(CACHE, "bin", "s1-rust"), "gix", url, repo],
            "repo_config": {"protocol.version": proto},
        })
    cfgs.append({
        "id": "libgit2", "client": "libgit2", "version": git2_ver, "protocol": "default",
        "variant": "default", "transport": "http",
        "argv": lambda sd, url, repo: [os.path.join(CACHE, "bin", "s1-rust"), "git2", url, repo],
        "repo_config": {},
    })
    cfgs.append({
        "id": "go-git", "client": "go-git", "version": _go_git_version(), "protocol": "default",
        "variant": "default", "transport": "http",
        "argv": lambda sd, url, repo: [os.path.join(CACHE, "bin", "s1-gogit"), url, repo],
        "repo_config": {},
    })
    return cfgs


def _jgit_version() -> str:
    m = re.search(r"JGIT_VERSION=(\S+)", open(os.path.join(HERE, "setup.sh")).read())
    return m.group(1) if m else "?"


def _cargo_versions() -> tuple[str, str]:
    lock = os.path.join(HERE, "harness-rust", "Cargo.lock")
    found = {}
    if os.path.exists(lock):
        for name, ver in re.findall(r'name = "([^"]+)"\nversion = "([^"]+)"', open(lock).read()):
            found[name] = ver
    g2 = found.get("git2", "?")
    if "libgit2-sys" in found:
        g2 += f" (libgit2-sys {found['libgit2-sys']})"
    return "gix " + found.get("gix", "?"), "git2 " + g2


def _go_git_version() -> str:
    m = re.search(r"go-git/go-git/v5 (v\S+)", open(os.path.join(HERE, "harness-go", "go.mod")).read())
    return m.group(1) if m else "?"


MEM_LIMIT = 4 << 30  # bytes of address space per client process; a delta cycle can eat all RAM
# The JVM and Go reserve large virtual ranges up front, so RLIMIT_AS is only applied to native clients.
MEM_LIMITED = {"git", "libgit2", "gitoxide"}


def _limit_memory():
    resource.setrlimit(resource.RLIMIT_AS, (MEM_LIMIT, MEM_LIMIT))


def run(cmd, cwd=None, env=None, timeout=TIMEOUT, input=None, limit_memory=False):
    return subprocess.run(cmd, cwd=cwd, env=env, timeout=timeout, capture_output=True, input=input,
                          preexec_fn=_limit_memory if limit_memory else None)


def prepare_repo(repo: str, meta: dict, packs_dir: str, cfg: dict, env: dict) -> None:
    run([INIT_GIT, "init", "-q", "--initial-branch=main", repo], env=env).check_returncode()
    for k, v in cfg["repo_config"].items():
        run([INIT_GIT, "-C", repo, "config", k, v], env=env).check_returncode()
    seed = meta["client_has"]
    if seed:
        for ext in ("pack", "idx"):
            shutil.copy(os.path.join(packs_dir, f"{seed}.{ext}"), os.path.join(repo, ".git", "objects", "pack"))
        for ref in ("refs/heads/main", "refs/remotes/origin/main"):
            run([INIT_GIT, "-C", repo, "update-ref", ref, meta["client_tip"]], env=env).check_returncode()


def verify(repo: str, meta: dict, env: dict) -> tuple[bool, str, int]:
    """Return (ok, reason, duplicate idx entries)."""
    dups = 0
    pack_dir = os.path.join(repo, ".git", "objects", "pack")
    for f in sorted(os.listdir(pack_dir)) if os.path.isdir(pack_dir) else []:
        if f.endswith(".idx"):
            out = run([REF_GIT, "show-index"], env=env, input=open(os.path.join(pack_dir, f), "rb").read())
            oids = [line.split()[1] for line in out.stdout.decode().splitlines()]
            dups += len(oids) - len(set(oids))
    tip = run([REF_GIT, "-C", repo, "rev-parse", "-q", "--verify", "refs/remotes/origin/main"], env=env)
    if tip.stdout.decode().strip() != meta["tip"]:
        return False, "refs/remotes/origin/main not at expected tip", dups
    try:
        fsck = run([REF_GIT, "-C", repo, "fsck", "--strict", "--full", "--no-dangling"], env=env, limit_memory=True)
    except subprocess.TimeoutExpired:
        return False, f"fsck did not finish in {TIMEOUT}s", dups
    if fsck.returncode < 0:
        return False, f"fsck killed by signal {-fsck.returncode} (memory limit {MEM_LIMIT >> 30} GiB)", dups
    if fsck.returncode != 0:
        msg = last_line(fsck.stderr or fsck.stdout) or f"exit {fsck.returncode}"
        if "out of memory" in msg.lower():
            msg = f"fsck ran out of memory (limit {MEM_LIMIT >> 30} GiB): {msg}"
        return False, "fsck: " + msg, dups
    n = run([REF_GIT, "-C", repo, "rev-list", "--objects", "refs/remotes/origin/main"], env=env)
    count = len(n.stdout.decode().splitlines())
    if count != meta["expected_objects"]:
        return False, f"{count} reachable objects, expected {meta['expected_objects']}", dups
    return True, "", dups


def last_line(b: bytes) -> str:
    lines = [ln for ln in b.decode(errors="replace").splitlines() if ln.strip()]
    tail = ([ln for ln in lines if re.match(r"\s*(fatal|error)", ln)]
            or [ln for ln in lines if re.search(r"(Error|Exception)", ln)] or lines)
    return tail[0].strip()[:300] if tail else ""  # the first error is the cause; later ones are fallout


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", default=os.path.join(tempfile.gettempdir(), "aspen-s1-work"))
    ap.add_argument("--only-clients", default="")
    ap.add_argument("--only-scenarios", default="")
    ap.add_argument("--out", default=HERE)
    a = ap.parse_args()
    work = os.path.abspath(a.work)
    os.makedirs(work, exist_ok=True)

    subprocess.run([sys.executable, os.path.join(HERE, "make_scenarios.py"), "--git", REF_GIT, "--out", work],
                   check=True, stdout=subprocess.DEVNULL)
    summary = json.load(open(os.path.join(work, "scenarios.json")))
    scen_root = os.path.join(work, "scenarios")
    packs_dir = os.path.join(work, "packs")
    scenarios = [s for s in summary["scenarios"] if re.search(a.only_scenarios, s["name"])]
    cfgs = [c for c in client_configs() if re.search(a.only_clients, c["id"])]

    srv = fake_server.start_http(scen_root)
    base_url = f"http://127.0.0.1:{srv.server_address[1]}"
    home = os.path.join(work, "home")
    os.makedirs(home, exist_ok=True)
    env = git_env(home)

    rows = []
    t0 = time.time()
    for cfg in cfgs:
        for meta in scenarios:
            sd = os.path.join(scen_root, meta["name"])
            repo = os.path.join(work, "runs", "repo")
            shutil.rmtree(repo, ignore_errors=True)
            prepare_repo(repo, meta, packs_dir, cfg, env)
            logf = os.path.join(work, "runs", "server.log")
            if os.path.exists(logf):
                os.remove(logf)
            renv = dict(env, S1_LOG=logf)
            os.environ["S1_LOG"] = logf  # the in-process HTTP server reads it per request
            url = f"{base_url}/{meta['name']}.git"
            try:
                p = run(cfg["argv"](sd, url, repo), cwd=repo, env=renv, limit_memory=cfg["client"] in MEM_LIMITED)
                code, err = p.returncode, last_line(p.stderr)
                if code < 0 or (not err and code != 0):
                    err = (f"killed by signal {-code}" if code < 0 else f"exit {code}, no message") + (
                        f"; memory capped at {MEM_LIMIT >> 30} GiB" if cfg["client"] in MEM_LIMITED else "")
                elif "memory allocation failed" in err.lower() or "out of memory" in err.lower():
                    err = f"ran out of memory (capped at {MEM_LIMIT >> 30} GiB): {err}"
            except subprocess.TimeoutExpired:
                code, err = None, f"timeout after {TIMEOUT}s"
            served = [json.loads(ln) for ln in open(logf)] if os.path.exists(logf) else []
            if code is None:
                outcome, reason, dups = "TIMEOUT", err, 0
            elif code != 0:
                ok, _, dups = verify(repo, meta, env)
                outcome, reason = "REJECT", err
            else:
                ok, why, dups = verify(repo, meta, env)
                outcome, reason = ("ACCEPT", "") if ok else ("CORRUPT", why)
            caps = served[-1]["caps"] if served else []
            rows.append({
                "client": cfg["client"], "version": cfg["version"], "config": cfg["id"],
                "protocol_configured": cfg["protocol"],
                "protocol_observed": served[-1]["proto"] if served else "-",
                "transport": cfg["transport"], "variant": cfg["variant"], "scenario": meta["name"],
                "expect": meta["expect"], "outcome": outcome, "idx_duplicates": dups,
                "thin_pack_requested": "thin-pack" in caps, "ofs_delta_requested": "ofs-delta" in caps,
                "caps": " ".join(caps), "detail": reason,
            })
            print(f"{cfg['id']:38} {meta['name']:20} {outcome:8} {reason[:90]}", flush=True)
    srv.shutdown()
    print(f"{len(rows)} runs in {time.time() - t0:.0f}s")

    cols = list(rows[0].keys())
    with open(os.path.join(a.out, "results.tsv"), "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(str(r[c]).replace("\t", " ") for c in cols) + "\n")
    write_matrix(rows, summary, scenarios, os.path.join(a.out, "MATRIX.md"))


CELL = {"ACCEPT": "ok", "REJECT": "**REJECT**", "CORRUPT": "**CORRUPT**", "TIMEOUT": "**TIMEOUT**"}


def write_matrix(rows, summary, scenarios, path):
    names = [s["name"] for s in scenarios]
    by_cfg = collections.OrderedDict()
    for r in rows:
        by_cfg.setdefault(r["config"], {})[r["scenario"]] = r
    out = ["# S1 matrix (generated by `run_matrix.py`; do not edit by hand)", ""]
    out.append("Cell values: `ok` = client succeeded and the repository passes `git fsck --strict --full` "
               "with the expected tip and object count; `REJECT` = client failed; `CORRUPT` = client "
               "reported success but the repository does not verify. `(dupN)` = N duplicate object ids "
               "written into the client's .idx files. `control-bad-count` is a negative control: "
               "`REJECT` is the correct result there.")
    out.append("")
    out.append("## Scenarios")
    out.append("")
    out.append("Counts describe the input segments; the last two columns describe the pack actually sent. "
               "`Hidden cycles` = deltas that only resolve through an external copy of an object the pack also "
               "contains (a delta cycle that `--fix-thin` hides).")
    out.append("")
    out.append("| Scenario | Stitcher | Segments | Client has | Entries in | Duplicates in | Cross-segment REF_DELTA | Forward REF_DELTA | Thin REF_DELTA | Entries sent | Hidden cycles | Description |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for s in scenarios:
        p = s["properties"]
        st = p.get("stitcher", {})
        sent = st.get("entries_out", p["entries"] - st.get("dropped_duplicates", 0))
        out.append(f"| `{s['name']}` | {s['stitcher']} | {'+'.join(s['segments'])} | {s['client_has'] or '-'} | {p['entries']} | "
                   f"{p['duplicate_entries']} | {p['cross_segment_ref_deltas']} | {p['forward_ref_deltas']} | "
                   f"{p['thin_ref_deltas']} | {sent} | {p.get('shadowed_thin_bases', '-')} | {s['description']} |")
    out.append("")
    out.append("## Results")
    out.append("")
    out.append("| Client config | Version | Protocol seen | " + " | ".join(f"`{n}`" for n in names) + " |")
    out.append("|---|---|---|" + "---|" * len(names))
    for cfg, cells in by_cfg.items():
        any_r = next(iter(cells.values()))
        vals = []
        for n in names:
            r = cells.get(n)
            if not r:
                vals.append("-")
                continue
            v = CELL[r["outcome"]]
            if r["idx_duplicates"]:
                v += f" (dup{r['idx_duplicates']})"
            vals.append(v)
        out.append(f"| {cfg} | {any_r['version']} | {any_r['protocol_observed']} | " + " | ".join(vals) + " |")
    out.append("")
    out.append("## Capabilities each client requested")
    out.append("")
    out.append("| Client config | Protocol seen | thin-pack | ofs-delta | All requested |")
    out.append("|---|---|---|---|---|")
    for cfg, cells in by_cfg.items():
        r = next((x for x in cells.values() if x["caps"]), next(iter(cells.values())))
        out.append(f"| {cfg} | {r['protocol_observed']} | {'yes' if r['thin_pack_requested'] else 'no'} | "
                   f"{'yes' if r['ofs_delta_requested'] else 'no'} | `{r['caps']}` |")
    out.append("")
    fails = [r for r in rows if r["outcome"] != "ACCEPT" and r["scenario"] != "control-bad-count"]
    bad_ctrl = [r for r in rows if r["scenario"] == "control-bad-count" and r["outcome"] == "ACCEPT"]
    out.append("## Failures (excluding the negative control)")
    out.append("")
    if not fails:
        out.append("None.")
    else:
        out.append("| Client config | Scenario | Outcome | Error |")
        out.append("|---|---|---|---|")
        for r in fails:
            out.append(f"| {r['config']} | `{r['scenario']}` | {r['outcome']} | {r['detail'].replace('|', '/')} |")
    if bad_ctrl:
        out.append("")
        out.append("**Negative control accepted (harness bug):** " + ", ".join(r["config"] for r in bad_ctrl))
    out.append("")
    with open(path, "w") as f:
        f.write("\n".join(out))


if __name__ == "__main__":
    main()
