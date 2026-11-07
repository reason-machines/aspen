#!/usr/bin/env python3
"""Bounded, open-loop stock-Git push load against a place canvas. No network without --execute.

Each offer is one `git push` process (smart HTTP: advertisement, then receive-pack)
of one new commit that changes one tile on a lane's own `paint/` branch. The
schedule is `place_load.offers`, so the ramp and hold match the API runner.
Results use the API runner's result shape, so `aggregate.py` merges them
(as their own group: run them under their own `--run`).
"""

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit

from place_load import PHASES, PALETTE, Phase, offers

ZERO = "0" * 40


@dataclass(frozen=True)
class Config:
    target_url: str = ""
    execute: bool = False
    acknowledge_persona_impact: bool = False
    worker: int = 0
    workers: int = 3
    rate: int = 67
    lanes: int = 400
    ramp_seconds: float = 60.0
    hold_seconds: float = 300.0
    deadline_seconds: float = 480.0
    request_timeout: float = 30.0
    start_at: float | None = None
    run: str = "aspen-git-load"
    max_inflight: int = 96
    max_lag_seconds: float = 1.0
    workdir: str = ""
    total_offered: int = 0

    def validate(self):
        numbers = (self.ramp_seconds, self.hold_seconds, self.deadline_seconds,
                   self.request_timeout, self.max_lag_seconds)
        if any(not math.isfinite(n) for n in numbers):
            raise ValueError("durations must be finite")
        if (self.ramp_seconds < 0 or self.hold_seconds <= 0
                or not 0 < self.deadline_seconds <= 900
                or not 0 < self.request_timeout <= 120
                or self.max_lag_seconds < 0 or not 1 <= self.max_inflight <= 1024):
            raise ValueError("invalid durations or inflight limit; deadline is at most 900s")
        if self.start_at is not None and not math.isfinite(self.start_at):
            raise ValueError("start-at must be a finite Unix timestamp")
        if not 0 <= self.total_offered <= 2000:
            raise ValueError("total-offered must be 0..2000")
        if not 0 <= self.worker < self.workers <= 64:
            raise ValueError("worker must be within workers (at most 64)")
        if not 1 <= self.rate <= 200 or not 1 <= self.lanes <= 5000:
            raise ValueError("rate must be 1..200 and lanes 1..5000")
        # Per-branch quota is 30 pushes a minute: a lane must not be offered more often.
        if self.rate * 60 / self.lanes > 30:
            raise ValueError("lanes too few: each branch may take at most 30 pushes a minute")
        if self.ramp_seconds + self.hold_seconds + self.request_timeout >= self.deadline_seconds:
            raise ValueError("deadline must leave time for setup, schedule and drain")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", self.run):
            raise ValueError("run must contain 1..80 letters, digits, hyphens or underscores")
        total = self.total_offered or self.rate * self.workers
        if self.execute and total > 500 and not self.acknowledge_persona_impact:
            raise ValueError("execution above 500 total offers/s requires --acknowledge-persona-impact")
        if self.execute:
            url = urlsplit(self.target_url)
            if (url.scheme != "https" and url.hostname not in ("127.0.0.1", "localhost")) or url.username \
                    or url.password or url.query or url.fragment or not url.path.endswith(".git"):
                raise ValueError("target must be an HTTPS .git URL without credentials or query")

    def public(self):
        return {k: v for k, v in vars(self).items() if k not in ("target_url", "workdir")}


def classify(returncode, porcelain, trace):
    """Reason for a push that did not land, from git's porcelain output and curl trace."""
    statuses = re.findall(r"<= Recv header: HTTP/[0-9.]+ (\d{3})", trace)
    bad = [s for s in statuses if s not in ("200", "100")]
    if bad:
        status = bad[-1]
        reason = "http_" + status
        if status in ("429", "503") and not re.search(r"<= Recv header: retry-after:", trace, re.I):
            reason += ":no_retry_after"
        return reason
    for line in porcelain.splitlines():
        if line.startswith("!"):
            message = line.split("\t", 2)[-1]
            if "place is at its limit" in message:
                return "git_rejected:global_ceiling" + ("" if "retry in" in message else ":no_retry_hint")
            if "push" in message and "minute" in message:
                return "git_rejected:branch_quota"
            if "busy" in message or "memory" in message:
                return "git_rejected:busy"
            return "git_rejected:other"
    return "git_exit_%d" % returncode


class Lane:
    __slots__ = ("branch", "tip", "index", "busy", "pushed", "retired")

    def __init__(self, branch, tip, index):
        self.branch, self.tip, self.index, self.busy, self.pushed = branch, tip, index, False, 0
        # A push whose outcome is unknown (timeout, transport error, an unexpected
        # exit) may have landed: the lane's remote tip is then unknown, so the lane
        # takes no more offers rather than risk a non-fast-forward on a stale tip.
        self.retired = False


# Failures where the server refused before applying anything: the remote tip is
# still the lane's tip, so the lane stays in service.
DEFINITE_REFUSALS = ("git_rejected:", "http_429", "http_503")


def kill_group(process):
    """SIGKILL the child's whole process group (git's transport helpers inherit our pipes)."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            process.kill()
        except ProcessLookupError:
            pass


async def run_process(argv, env=None, stdin=None, timeout=None):
    """Run argv in its own process group; on timeout or cancellation kill the whole group and
    reap the child before re-raising, so nothing it started outlives the call.

    The child is created with a synchronous Popen, so the handle exists the moment the fork
    returns: no cancellation or deadline can land between creating the child and holding it.
    The deadline is fixed before the spawn, so a slow spawn spends the same budget. The pipes
    are drained on a dedicated thread; cleanup waits for that thread through any number of
    further cancellations, so the call never returns while the child or its pipes are live."""
    if timeout is not None and timeout <= 0:
        raise asyncio.TimeoutError()
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    process = subprocess.Popen(
        argv, env=env, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    done = loop.create_future()

    def drain():
        try:
            result = process.communicate(stdin)
        except BaseException as error:  # noqa: BLE001 - handed to the loop
            loop.call_soon_threadsafe(lambda: done.done() or done.set_exception(error))
        else:
            loop.call_soon_threadsafe(lambda: done.done() or done.set_result(result))

    threading.Thread(target=drain, name="git_load-child", daemon=True).start()
    try:
        left = None if deadline is None else deadline - loop.time()
        if left is not None and left <= 0:
            raise asyncio.TimeoutError()
        out, err = await asyncio.wait_for(asyncio.shield(done), left)
    except BaseException:
        kill_group(process)
        cancelled = False
        while not done.done():  # the drain ends once the group is dead; repeated cancels wait for it
            try:
                await asyncio.shield(done)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        process.wait()
        if cancelled:
            raise asyncio.CancelledError() from None
        raise
    return process.returncode, out.decode(errors="replace"), err.decode(errors="replace")


class Runner:
    def __init__(self, config):
        self.config = config
        self.phases = {name: Phase() for name in PHASES}
        self.pending = set()
        self.max_lag_ms = 0
        self.fatal = None
        self.start_at = None
        self.scheduling_finished = False
        self.landed = {}
        self.retired_lanes = 0
        self.repo = None
        self.deadline = None  # monotonic; every child process is bounded by what is left of it

    def remaining(self, timeout=None):
        if self.deadline is None:
            return timeout
        left = self.deadline - time.monotonic()
        return left if timeout is None else min(timeout, left)

    async def git(self, *args, env=None, stdin=None, timeout=None):
        return await run_process(("git", "-C", str(self.repo), *args), env=env, stdin=stdin,
                                 timeout=self.remaining(timeout))

    async def setup(self):
        root = Path(self.config.workdir or tempfile.mkdtemp(prefix="git-load-"))
        self.repo = root / "canvas"
        code, _, _ = await run_process(
            ("git", "-c", "protocol.version=2", "clone", "-q", "--no-checkout", "--depth", "1", "--single-branch",
             "--branch", "main", self.config.target_url, str(self.repo)), timeout=self.remaining())
        if code:
            raise RuntimeError("clone_failed")
        for key, value in (("user.name", "aspen load test"), ("user.email", "load@aspen.invalid"),
                           ("gc.auto", "0"), ("maintenance.auto", "false"), ("push.negotiate", "false"),
                           ("core.fsync", "none"), ("credential.helper", "")):
            await self.git("config", key, value)
        _, base, _ = await self.git("rev-parse", "origin/main")
        base = base.strip()
        _, tree, _ = await self.git("rev-parse", base + "^{tree}")
        self.lanes = []
        for n in range(self.config.lanes):
            index = root / ("index-%d" % n)
            env = {**os.environ, "GIT_INDEX_FILE": str(index)}
            if n == 0:
                code, _, _ = await self.git("read-tree", base, env=env)
                if code:
                    raise RuntimeError("read_tree_failed")
                template = index.read_bytes()
            else:
                index.write_bytes(template)
            branch = "paint/%s-w%d-%d" % (self.config.run, self.config.worker, n)
            self.lanes.append(Lane(branch, base, index))
        self.blobs = {}
        for color in PALETTE:
            _, oid, _ = await self.git("hash-object", "-w", "--stdin", stdin=(color + "\n").encode())
            self.blobs[color] = oid.strip()

    async def push(self, number, phase, lane):
        stats = self.phases[phase]
        config = self.config
        x = 16 + (number + config.worker * 7) % 16
        y = (number // 16 + config.worker * 5) % 16
        color = PALETTE[(number // config.lanes + config.worker) % len(PALETTE)]
        env = {**os.environ, "GIT_INDEX_FILE": str(lane.index)}
        commit = None
        try:
            await self.git("update-index", "--add", "--cacheinfo",
                           "100644,%s,tiles/%d_%d.txt" % (self.blobs[color], x, y), env=env)
            _, tree, _ = await self.git("write-tree", env=env)
            _, commit, _ = await self.git("commit-tree", tree.strip(), "-p", lane.tip, "-m",
                                          "%s:%d:%d" % (config.run, config.worker, number))
            commit = commit.strip()
            trace_env = {**os.environ, "GIT_TRACE_CURL": "1", "GIT_TRACE_CURL_NO_DATA": "1",
                         "GIT_TERMINAL_PROMPT": "0"}
            before = time.monotonic()
            code, out, err = await self.git("-c", "protocol.version=2", "push", "--porcelain", "origin",
                                            "%s:refs/heads/%s" % (commit, lane.branch),
                                            env=trace_env, timeout=config.request_timeout)
            elapsed = time.monotonic() - before
            if code == 0 and re.search(r"^[ *+]\t", out, re.M):
                lane.tip = commit
                lane.pushed += 1
                self.landed[lane.branch] = commit
                stats.success += 1
                stats.latency_histogram_ms[max(1, math.ceil(elapsed * 1000))] += 1
            else:
                reason = classify(code, out, err)
                if not reason.startswith(DEFINITE_REFUSALS):
                    reason += ":outcome_unknown"
                    self.retire(lane)
                stats.failures[reason] += 1
        except asyncio.CancelledError:
            # Retired, so its index is never used again: no restore, nothing outlives the deadline.
            stats.failures["deadline_cancelled_outcome_unknown"] += 1
            self.retire(lane)
            lane.busy = False
            raise
        except (TimeoutError, asyncio.TimeoutError):
            stats.failures["timeout_outcome_unknown"] += 1
            self.retire(lane)
        except Exception:
            stats.failures["transport_error_outcome_unknown"] += 1
            self.retire(lane)
        if lane.tip != commit:
            # The commit is not the lane's tip: put the lane's index back to the tip's tree so a
            # later commit does not carry this one's tile. Not shielded and bounded by the run's
            # deadline; a restore that cannot finish retires the lane.
            try:
                await self.restore_index(lane)
            finally:
                lane.busy = False
        else:
            lane.busy = False

    def retire(self, lane):
        if not lane.retired:
            lane.retired = True
            self.retired_lanes += 1

    async def restore_index(self, lane):
        try:
            code, _, _ = await self.git("read-tree", lane.tip, env={**os.environ, "GIT_INDEX_FILE": str(lane.index)})
        except asyncio.CancelledError:
            self.retire(lane)
            raise
        except Exception:
            code = 1
        if code:
            self.retire(lane)

    async def work(self, deadline):
        config = self.config
        self.start_at = config.start_at if config.start_at is not None else time.time() + 20
        start = time.monotonic() + self.start_at - time.time()
        if start + config.ramp_seconds + config.hold_seconds + config.request_timeout > deadline:
            self.fatal = "schedule_and_drain_do_not_fit_deadline"
            return
        try:
            await self.setup()
        except Exception:
            self.fatal = "setup_failed"
            return
        if time.monotonic() >= start:
            self.fatal = "setup_missed_common_start"
            return
        for number, phase, offset in offers(config):
            due = start + offset
            await asyncio.sleep(max(0, due - time.monotonic()))
            stats = self.phases[phase]
            stats.scheduled += 1
            lag = time.monotonic() - due
            self.max_lag_ms = max(self.max_lag_ms, math.ceil(lag * 1000))
            lane = self.lanes[number % config.lanes]
            if time.monotonic() >= start + config.ramp_seconds + config.hold_seconds:
                stats.skipped["window_ended"] += 1
            elif lag > config.max_lag_seconds:
                stats.skipped["scheduler_lag"] += 1
            elif lane.retired:
                stats.skipped["lane_retired_outcome_unknown"] += 1
            elif lane.busy:
                stats.skipped["lane_busy"] += 1
            elif len(self.pending) >= config.max_inflight:
                stats.skipped["inflight_limit"] += 1
            else:
                lane.busy = True
                stats.sent += 1
                task = asyncio.create_task(self.push(number, phase, lane))
                self.pending.add(task)
                task.add_done_callback(self.pending.discard)
        self.scheduling_finished = True
        if self.pending:
            await asyncio.gather(*list(self.pending))

    async def verify(self):
        """Check each lane's last acknowledged commit with stock git, in two separate ways.

        Earlier acknowledged commits on a lane are not checked one by one; they are
        covered only as ancestors of the last one.

        `advertised`: the server's ref advertisement (`ls-remote`). A lane in
        service must advertise exactly its last acknowledged commit. A retired
        lane's last push had an unknown outcome, so its branch may be one commit
        newer: it must still exist, and a tip other than the acknowledged one is
        counted as `unknown_newer`, not as a match. A missing branch is a
        mismatch either way. `fetched`: a fresh repository fetches every branch
        with an acknowledged push (retired ones included) two commits deep, and
        each branch's last acknowledged commit and its tree must be present
        there. A lane has at most one push in flight, so an acknowledged commit
        that is still on its branch is within two commits of the tip.
        """
        retired = {lane.branch for lane in self.lanes if lane.retired}
        expected = dict(self.landed)
        result = {"branches_acknowledged": len(expected),
                  "branches_with_unknown_last_outcome": len(retired & expected.keys())}
        try:
            code, out, _ = await self.git("ls-remote", "origin",
                                          "refs/heads/paint/%s-w%d-*" % (self.config.run, self.config.worker))
        except Exception:
            code, out = 1, ""
        if code:
            result["advertised"] = {"status": "failed"}
        else:
            remote = dict(reversed(line.split("\t")) for line in out.splitlines() if "\t" in line)
            matches = unknown_newer = bad = 0
            for branch, commit in expected.items():
                tip = remote.get("refs/heads/" + branch)
                if tip == commit:
                    matches += 1
                elif tip is not None and branch in retired:
                    unknown_newer += 1
                else:
                    bad += 1
            result["advertised"] = {"status": "ok" if not bad else "mismatch", "checked": len(expected),
                                    "tip_matches": matches, "unknown_newer": unknown_newer}
        result["fetched"] = await self.verify_fetched(expected)
        return result

    async def verify_fetched(self, expected):
        if not expected:
            return {"status": "ok", "checked": 0, "commits_present": 0}
        fresh = Path(tempfile.mkdtemp(prefix="git-load-verify-"))
        try:
            code, _, _ = await run_process(("git", "init", "-q", "--bare", str(fresh)))
            if code:
                return {"status": "failed", "checked": len(expected), "commits_present": 0}
            specs = ["+refs/heads/%s:refs/verify/%d" % (b, n) for n, b in enumerate(expected)]
            code, _, _ = await run_process(("git", "-C", str(fresh), "-c", "protocol.version=2", "fetch", "-q",
                                            "--depth", "2", self.config.target_url, *specs))
            if code:
                return {"status": "failed", "checked": len(expected), "commits_present": 0}
            stdin = "".join("%s\n%s^{tree}\n" % (c, c) for c in expected.values()).encode()
            code, out, _ = await run_process(("git", "-C", str(fresh), "cat-file", "--batch-check"), stdin=stdin)
            lines = out.splitlines()
            present = sum(1 for n in range(0, len(lines) - 1, 2)
                          if " commit " in lines[n] and " tree " in lines[n + 1])
            return {"status": "ok" if present == len(expected) else "mismatch",
                    "checked": len(expected), "commits_present": present}
        except (asyncio.CancelledError, TimeoutError, asyncio.TimeoutError):
            raise
        except Exception:
            return {"status": "failed", "checked": len(expected), "commits_present": 0}
        finally:
            shutil.rmtree(fresh, ignore_errors=True)

    async def run(self):
        """Work, drain and verify under one deadline (`deadline_seconds` from the start of run)."""
        began = time.monotonic()
        deadline = self.deadline = began + self.config.deadline_seconds
        try:
            await asyncio.wait_for(self.work(deadline), self.config.deadline_seconds)
        except (TimeoutError, asyncio.TimeoutError):
            self.fatal = "deadline_exceeded"
        finally:
            pending = list(self.pending)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        verification = {"status": "not_run"}
        if not self.fatal and self.landed:
            try:
                verification = await asyncio.wait_for(self.verify(), max(0, deadline - time.monotonic()))
            except (TimeoutError, asyncio.TimeoutError):
                verification = {"status": "failed:deadline"}
                self.fatal = "verification_deadline_exceeded"
            else:
                failed = [k for k in ("advertised", "fetched") if verification[k]["status"] != "ok"]
                verification["status"] = "failed:" + ",".join(failed) if failed else "ok"
                if failed:
                    self.fatal = "verification_failed"
        return {"schema": "aspen-place-load-v1", "kind": "stock-git", "worker": self.config.worker,
                "run": self.config.run, "config": self.config.public(), "start": self.start_at,
                "elapsed_seconds": round(time.monotonic() - began, 3), "fatal": self.fatal,
                "scheduling_finished": self.scheduling_finished, "max_schedule_lag_ms": self.max_lag_ms,
                "unique_successful_commits": sum(lane.pushed for lane in getattr(self, "lanes", [])),
                "retired_lanes": self.retired_lanes, "verification": verification,
                "phases": {name: phase.result() for name, phase in self.phases.items()}}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    defaults = Config()
    for name in ("target-url", "worker", "workers", "rate", "lanes", "ramp-seconds", "hold-seconds",
                 "deadline-seconds", "request-timeout", "start-at", "run", "max-inflight",
                 "max-lag-seconds", "workdir", "total-offered"):
        attr = name.replace("-", "_")
        default = getattr(defaults, attr)
        kind = float if attr == "start_at" else type(default)
        parser.add_argument("--" + name, type=kind,
                            default=os.getenv("ASPEN_GIT_LOAD_" + attr.upper(), default))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--acknowledge-persona-impact", action="store_true")
    args = parser.parse_args(argv)
    config = Config(**{k: (type(getattr(defaults, k))(v) if v is not None and getattr(defaults, k) is not None else
                           (float(v) if v is not None else None)) for k, v in vars(args).items()})
    try:
        config.validate()
    except ValueError as error:
        parser.error(str(error))
    return config


def main(argv=None):
    config = parse_args(argv)
    if not config.execute:
        counts = Counter(phase for _, phase, _ in offers(config))
        print(json.dumps({"dry_run": True, "config": config.public(), "scheduled_per_worker": dict(counts)}))
        return 0
    result = asyncio.run(Runner(config).run())
    print(json.dumps({"result": result}, separators=(",", ":")), flush=True)
    return 2 if result["fatal"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
