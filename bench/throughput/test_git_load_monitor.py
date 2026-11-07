"""Local-only tests for git_load.py (a bare repository on disk) and monitor.py (events fed directly)."""

import asyncio
import dataclasses
import io
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import aggregate
import git_load
import monitor


def sh(*args, cwd=None):
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class GitLoadConfigTests(unittest.TestCase):
    """Every numeric guard at its boundary: the first bad value is refused, the last good one taken."""

    BASE = git_load.Config(rate=1, lanes=10, ramp_seconds=0, hold_seconds=1, request_timeout=1, deadline_seconds=900)
    NAN, INF = float("nan"), float("inf")
    GUARDS = {
        "ramp_seconds": ([-0.001, INF, NAN], [0]),
        "hold_seconds": ([0, -5, INF, NAN], [0.001]),
        "deadline_seconds": ([0, -1, 900.001, INF, NAN], [900]),
        "request_timeout": ([0, -1, 120.001, INF, NAN], [120]),
        "max_lag_seconds": ([-0.001, INF, NAN], [0]),
        "max_inflight": ([0, 1025], [1, 1024]),
        "start_at": ([INF, NAN], [None, 1.0]),
        "total_offered": ([-1, 2001], [0, 2000]),
        "lanes": ([0, 1, 5001], [2, 5000]),  # at rate 1, one lane would take 60 pushes a minute
    }

    def check(self, change, ok):
        config = replace(self.BASE, **change)
        with self.subTest(change=change, ok=ok):
            if ok:
                config.validate()
            else:
                with self.assertRaises(ValueError):
                    config.validate()

    def test_each_numeric_guard_at_its_boundary(self):
        self.BASE.validate()
        for field, (bad, good) in self.GUARDS.items():
            for value in bad:
                self.check({field: value}, False)
            for value in good:
                self.check({field: value}, True)

    def test_rate_worker_quota_and_schedule_guards(self):
        for change, ok in (({"rate": 0}, False), ({"rate": 201, "lanes": 500}, False),
                           ({"rate": 200, "lanes": 400}, True), ({"rate": 30, "lanes": 60}, True),
                           ({"rate": 30, "lanes": 59}, False), ({"rate": 1, "lanes": 1}, False),
                           ({"rate": 1, "lanes": 2}, True), ({"worker": -1}, False),
                           ({"worker": 3, "workers": 3}, False), ({"workers": 65}, False),
                           ({"worker": 63, "workers": 64}, True),
                           ({"hold_seconds": 10, "request_timeout": 10, "deadline_seconds": 20}, False),
                           ({"hold_seconds": 10, "request_timeout": 10, "deadline_seconds": 20.001}, True)):
            self.check(change, ok)


class GitLoadRunnerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.remote = root / "canvas.git"
        seed = root / "seed"
        sh("git", "init", "-q", "--bare", "-b", "main", str(self.remote))
        sh("git", "init", "-q", "-b", "main", str(seed))
        (seed / "tiles").mkdir()
        (seed / "tiles" / "0_0.txt").write_text("#ffffff\n")
        sh("git", "add", ".", cwd=seed)
        sh("git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "seed", cwd=seed)
        sh("git", "push", "-q", str(self.remote), "main", cwd=seed)
        self.workdir = root / "work"
        self.workdir.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def config(self, **kwargs):
        config = git_load.Config(target_url=str(self.remote), execute=True, rate=20, lanes=1, ramp_seconds=0,
                                 hold_seconds=.5, request_timeout=5, deadline_seconds=30,
                                 start_at=time.time() + 1.5, workdir=str(self.workdir), max_lag_seconds=5)
        return replace(config, **kwargs)

    def remote_tip(self, branch):
        return sh("git", "--git-dir", str(self.remote), "rev-parse", "refs/heads/" + branch)

    def main_commit(self):
        return sh("git", "--git-dir", str(self.remote), "rev-parse", "main")

    def second_push_lands_then_times_out(self, runner):
        """Push 1 is acknowledged; push 2 reaches the server but the client sees a timeout."""
        real_git = runner.git
        state = {"pushes": 0}

        async def git(*args, **kwargs):
            if "push" in args:
                state["pushes"] += 1
                result = await real_git(*args, **kwargs)
                if state["pushes"] == 2:
                    raise asyncio.TimeoutError()
                return result
            return await real_git(*args, **kwargs)
        runner.git = git
        return state

    def before_verify(self, runner, action):
        real_verify = runner.verify

        async def verify():
            action()
            return await real_verify()
        runner.verify = verify

    async def test_pushes_land_and_both_read_back_checks_pass(self):
        runner = git_load.Runner(self.config(rate=4, lanes=2))
        result = await runner.run()
        self.assertIsNone(result["fatal"])
        self.assertEqual(result["verification"]["status"], "ok")
        self.assertEqual(result["verification"]["advertised"]["status"], "ok")
        self.assertEqual(result["verification"]["fetched"]["status"], "ok")
        self.assertGreater(result["verification"]["fetched"]["commits_present"], 0)
        for lane in runner.lanes:
            self.assertEqual(self.remote_tip(lane.branch), lane.tip)

    async def test_unknown_outcome_retires_lane_and_restores_its_index(self):
        """The push lands on the server, but the client times out: the lane must not be reused."""
        runner = git_load.Runner(self.config())
        real_git = runner.git
        state = {"pushes": 0}

        async def git(*args, **kwargs):
            if "push" in args:
                state["pushes"] += 1
                result = await real_git(*args, **kwargs)
                if state["pushes"] == 1:
                    raise asyncio.TimeoutError()
                return result
            return await real_git(*args, **kwargs)

        runner.git = git
        result = await runner.run()
        lane = runner.lanes[0]
        self.assertTrue(lane.retired)
        self.assertEqual(state["pushes"], 1)
        hold = result["phases"]["hold"]
        self.assertEqual(hold["failures"], {"timeout_outcome_unknown": 1})
        # Deterministic under load: however slow the push was, no later offer was sent;
        # each was skipped while the push ran (lane_busy) or after it (lane retired).
        self.assertEqual(hold["sent"], 1)
        self.assertEqual(set(hold["skipped"]) - {"lane_busy", "lane_retired_outcome_unknown"}, set())
        self.assertEqual(sum(hold["skipped"].values()), hold["scheduled"] - 1)
        index_tree = subprocess.run(
            ["git", "-C", str(runner.repo), "write-tree"], env={"GIT_INDEX_FILE": str(lane.index), "PATH": "/usr/bin:/bin"},
            check=True, capture_output=True, text=True).stdout.strip()
        self.assertEqual(index_tree, sh("git", "-C", str(runner.repo), "rev-parse", lane.tip + "^{tree}"))
        self.assertEqual(result["retired_lanes"], 1)

    async def test_definite_refusal_keeps_lane_and_restores_index(self):
        runner = git_load.Runner(self.config(rate=2, hold_seconds=2))
        real_git = runner.git
        state = {"pushes": 0}

        async def git(*args, **kwargs):
            if "push" in args:
                state["pushes"] += 1
                if state["pushes"] == 1:
                    return 1, "To x\n!\tc:refs/heads/b\t[remote rejected] (place is at its limit, retry in 1s)\nDone\n", ""
            return await real_git(*args, **kwargs)

        runner.git = git
        result = await runner.run()
        self.assertFalse(runner.lanes[0].retired)
        self.assertEqual(result["phases"]["hold"]["failures"], {"git_rejected:global_ceiling": 1})
        self.assertGreaterEqual(result["phases"]["hold"]["success"], 1)
        self.assertIsNone(result["fatal"])
        self.assertEqual(result["verification"]["status"], "ok")
        # The refused commit's tile must not ride along in the next commit: every landed
        # commit changes exactly one tile against its parent.
        branch = runner.lanes[0].branch
        landed = sh("git", "--git-dir", str(self.remote), "rev-list", "refs/heads/" + branch, "^main").split()
        self.assertEqual(len(landed), result["phases"]["hold"]["success"])
        for commit in landed:
            paths = sh("git", "--git-dir", str(self.remote), "diff-tree", "--no-commit-id", "--name-only", "-r",
                       commit).split()
            self.assertEqual(len(paths), 1, (commit, paths))

    async def test_mismatched_read_back_fails_the_run(self):
        runner = git_load.Runner(self.config(rate=2, hold_seconds=2))
        real_verify = runner.verify

        async def verify():
            for branch in runner.landed:
                sh("git", "--git-dir", str(self.remote), "update-ref", "refs/heads/" + branch,
                   sh("git", "--git-dir", str(self.remote), "rev-parse", "main"))
            return await real_verify()

        runner.verify = verify
        result = await runner.run()
        self.assertEqual(result["fatal"], "verification_failed")
        self.assertEqual(result["verification"]["advertised"]["status"], "mismatch")
        self.assertTrue(result["verification"]["status"].startswith("failed:"))

    async def test_failed_read_back_fails_run_and_aggregate_and_exit_code(self):
        runner = git_load.Runner(self.config(rate=2, hold_seconds=2))
        real_git = runner.git

        async def git(*args, **kwargs):
            if "ls-remote" in args:
                return 128, "", "fatal"
            return await real_git(*args, **kwargs)

        runner.git = git
        result = await runner.run()
        self.assertEqual(result["fatal"], "verification_failed")
        self.assertEqual(result["verification"]["advertised"]["status"], "failed")
        merged = aggregate.merge([result], expected_workers=3)
        self.assertFalse(merged["complete"])
        self.assertEqual(merged["verification"]["failed_workers"], [0])
        self.__class__.failed_result = result

    async def test_retired_lane_with_landed_unknown_push_verifies_its_acknowledged_commit(self):
        runner = git_load.Runner(self.config(rate=2, hold_seconds=4))
        self.second_push_lands_then_times_out(runner)
        result = await runner.run()
        lane = runner.lanes[0]
        self.assertTrue(lane.retired)
        self.assertNotEqual(self.remote_tip(lane.branch), lane.tip)  # the unknown push landed
        self.assertIsNone(result["fatal"])
        verification = result["verification"]
        self.assertEqual(verification["status"], "ok")
        self.assertEqual(verification["advertised"], {"status": "ok", "checked": 1, "tip_matches": 0,
                                                      "unknown_newer": 1})
        self.assertEqual(verification["fetched"], {"status": "ok", "checked": 1, "commits_present": 1})

    async def test_retired_lane_whose_branch_vanished_fails_verification(self):
        """Reviewer's repro: ack one push, time out the next, delete the branch."""
        runner = git_load.Runner(self.config(rate=2, hold_seconds=4))
        self.second_push_lands_then_times_out(runner)
        self.before_verify(runner, lambda: sh("git", "--git-dir", str(self.remote), "update-ref", "-d",
                                              "refs/heads/" + runner.lanes[0].branch))
        result = await runner.run()
        self.assertEqual(result["fatal"], "verification_failed")
        self.assertEqual(result["verification"]["advertised"]["status"], "mismatch")
        self.assertEqual(result["verification"]["advertised"]["checked"], 1)
        self.assertEqual(result["verification"]["fetched"]["status"], "failed")

    async def test_fresh_fetch_catches_an_acknowledged_commit_gone_from_a_retired_branch(self):
        """The advertisement is lenient for a retired lane; the fetched-object check is not."""
        runner = git_load.Runner(self.config(rate=2, hold_seconds=4))
        self.second_push_lands_then_times_out(runner)
        self.before_verify(runner, lambda: sh("git", "--git-dir", str(self.remote), "update-ref",
                                              "refs/heads/" + runner.lanes[0].branch, self.main_commit()))
        result = await runner.run()
        self.assertEqual(result["verification"]["advertised"]["status"], "ok")
        self.assertEqual(result["verification"]["fetched"], {"status": "mismatch", "checked": 1,
                                                             "commits_present": 0})
        self.assertEqual(result["fatal"], "verification_failed")
        self.assertEqual(result["verification"]["status"], "failed:fetched")

    async def test_verification_shares_the_run_deadline_and_kills_a_hung_fetch(self):
        pidfile = Path(self.tmp.name) / "fetch.pid"
        real = git_load.run_process

        async def run_process(argv, **kwargs):
            if "fetch" in argv:
                argv = ("sh", "-c", 'echo $$ > "$0"; exec sleep 60', str(pidfile))
            return await real(argv, **kwargs)

        config = self.config(rate=2, hold_seconds=.5, request_timeout=1, deadline_seconds=6, start_at=time.time() + 2)
        began = time.monotonic()
        with patch.object(git_load, "run_process", run_process):
            result = await git_load.Runner(config).run()
        self.assertLess(time.monotonic() - began, 6 + 1)
        self.assertEqual(result["fatal"], "verification_deadline_exceeded")
        self.assertEqual(result["verification"], {"status": "failed:deadline"})
        pid = int(pidfile.read_text())
        with self.assertRaises(ProcessLookupError):  # killed and reaped, not left as a zombie
            os.kill(pid, 0)


def gone(pid, wait=3.0):
    """True once pid no longer runs (absent or a zombie awaiting its new parent)."""
    end = time.monotonic() + wait
    while time.monotonic() < end:
        try:
            state = Path("/proc/%d/stat" % pid).read_text().rsplit(")", 1)[1].split()[0]
        except FileNotFoundError:
            return True
        if state in ("Z", "X"):
            return True
        time.sleep(.05)
    return False


class SubprocessDeadlineTests(unittest.IsolatedAsyncioTestCase):
    """Re-review 2: no child process, or anything it started, outlives the runner's deadline."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    async def test_timeout_kills_pipe_inheriting_descendants(self):
        pidfile = Path(self.tmp.name) / "grandchild.pid"
        began = time.monotonic()
        with self.assertRaises(asyncio.TimeoutError):
            # the background sleep inherits stdout/stderr, like git's transport helpers
            await git_load.run_process(("sh", "-c", 'sleep 30 & echo $! > "$0"; wait', str(pidfile)), timeout=.3)
        self.assertLess(time.monotonic() - began, 2)
        self.assertTrue(gone(int(pidfile.read_text())), "grandchild survived the timeout")

    async def test_repeated_cancellation_during_cleanup_still_kills_and_reaps(self):
        # Re-review 4: a second cancel while cleanup waits for the child must not escape it; the
        # call returns only after the group is dead and the child reaped, then propagates.
        pidfile = Path(self.tmp.name) / "grandchild.pid"
        real_kill = git_load.kill_group
        children = []

        def slow_kill(process):  # the kill lands only after both cancels have arrived
            children.append(process.pid)
            threading.Timer(.3, real_kill, (process,)).start()

        with patch.object(git_load, "kill_group", slow_kill):
            task = asyncio.ensure_future(git_load.run_process(
                ("sh", "-c", 'sleep 30 & echo $! > "$0"; wait', str(pidfile)), timeout=30))
            for _ in range(200):
                if pidfile.exists() and pidfile.read_text().strip():
                    break
                await asyncio.sleep(.01)
            task.cancel()
            await asyncio.sleep(.05)
            task.cancel()
            await asyncio.sleep(.05)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
        with self.assertRaises(ProcessLookupError):  # already killed and reaped when the call ended
            os.kill(children[0], 0)
        self.assertTrue(gone(int(pidfile.read_text())), "grandchild survived the cancellation")

    async def test_slow_spawn_spends_the_deadline(self):
        # Re-review 4: the deadline covers creating the child, not only talking to it.
        real_popen = git_load.subprocess.Popen
        children = []

        def slow_popen(*args, **kwargs):
            time.sleep(.2)  # a slow fork/exec blocks, like the real one
            process = real_popen(*args, **kwargs)
            children.append(process.pid)
            return process

        spawned_at = []
        real_slow = slow_popen

        def timed_popen(*args, **kwargs):
            process = real_slow(*args, **kwargs)
            spawned_at.append(time.monotonic())
            return process

        with patch.object(git_load.subprocess, "Popen", timed_popen):
            with self.assertRaises(asyncio.TimeoutError):
                await git_load.run_process(("sleep", "30"), timeout=.08)
        # The 80 ms were spent in the 200 ms spawn: no further wait once the child exists (a timer
        # started after the spawn would wait 80 ms more).
        self.assertLess(time.monotonic() - spawned_at[0], .06)
        with self.assertRaises(ProcessLookupError):  # killed and reaped before the call returned
            os.kill(children[0], 0)

    async def test_hung_setup_clone_is_killed_at_the_deadline(self):
        import socket
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(8)  # accepts, never answers: git-remote-http hangs in the clone
        port = server.getsockname()[1]
        try:
            config = git_load.Config(target_url="http://127.0.0.1:%d/canvas.git" % port, execute=True, rate=1,
                                     lanes=1, ramp_seconds=0, hold_seconds=.2, request_timeout=.2,
                                     deadline_seconds=1.5, start_at=time.time() + 1.0,
                                     workdir=str(Path(self.tmp.name) / "w"), max_lag_seconds=5)
            began = time.monotonic()
            result = await git_load.Runner(config).run()
            self.assertLess(time.monotonic() - began, 1.5 + 1)
            self.assertEqual(result["fatal"], "deadline_exceeded")
            await asyncio.sleep(.2)
            left = subprocess.run(["pgrep", "-f", "127.0.0.1:%d" % port], capture_output=True, text=True).stdout.split()
            self.assertEqual([p for p in left if not gone(int(p), wait=1)], [], "clone or its helper survived")
        finally:
            server.close()

    async def test_index_restore_is_bounded_by_the_deadline(self):
        root = Path(self.tmp.name)
        remote, seed = root / "canvas.git", root / "seed"
        sh("git", "init", "-q", "--bare", "-b", "main", str(remote))
        sh("git", "init", "-q", "-b", "main", str(seed))
        (seed / "a.txt").write_text("a\n")
        sh("git", "add", ".", cwd=seed)
        sh("git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "seed", cwd=seed)
        sh("git", "push", "-q", str(remote), "main", cwd=seed)
        pidfile = root / "restore.pid"
        real = git_load.run_process

        async def run_process(argv, **kwargs):
            if "push" in argv:  # a transport failure: outcome unknown, the index must be restored
                return 128, "", "fatal: unable to access"
            if "read-tree" in argv and pidfile.parent.joinpath("armed").exists():
                argv = ("sh", "-c", 'echo $$ > "$0"; exec sleep 30', str(pidfile))
            return await real(argv, **kwargs)

        config = git_load.Config(target_url=str(remote), execute=True, rate=1, lanes=1, ramp_seconds=0,
                                 hold_seconds=1, request_timeout=.5, deadline_seconds=3,
                                 start_at=time.time() + 600, workdir=str(root / "w"), max_lag_seconds=5)
        runner = git_load.Runner(config)
        # Setup (clone, config, lanes) runs before the timed run, so a slow, CPU-starved setup cannot
        # miss the common start and skip the push whose restore this test is about.
        await runner.setup()
        (root / "armed").touch()  # only the restore after the failed push hangs

        async def setup():
            pass
        runner.setup = setup
        runner.config = dataclasses.replace(config, start_at=time.time() + .3)
        began = time.monotonic()
        with patch.object(git_load, "run_process", run_process):
            result = await runner.run()
        # Bounded by the run's 3 s deadline; an unbounded restore would hang its 30 s sleep.
        self.assertLess(time.monotonic() - began, 3 + 1.5)
        self.assertTrue(pidfile.exists(), "the restore never ran")
        self.assertTrue(gone(int(pidfile.read_text()), wait=.05), "restore outlived the runner")
        self.assertTrue(runner.lanes[0].retired)
        self.assertEqual(result["phases"]["hold"]["sent"], 1)


class GitLoadExitCodeTests(unittest.TestCase):
    def test_verification_failure_exits_nonzero(self):
        async def run(_self):
            return {"fatal": "verification_failed"}
        with patch.object(git_load.Runner, "run", run), patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(git_load.main(["--execute", "--target-url", "https://x.invalid/c.git"]), 2)


class AggregateVerificationTests(unittest.TestCase):
    def row(self, worker, **extra):
        phase = {"scheduled": 1, "sent": 1, "success": 1, "skipped": {}, "failures": {}, "latency_histogram_ms": {"5": 1}}
        return {"worker": worker, "run": "r", "start": 1, "config": {"ramp_seconds": 0, "hold_seconds": 1, "rate": 1, "workers": 2},
                "fatal": None, "scheduling_finished": True, "phases": {"ramp": phase, "hold": phase}, **extra}

    def test_legacy_read_back_mismatch_makes_run_incomplete(self):
        rows = [self.row(0, read_back_branches=3, read_back_tip_matches=3),
                self.row(1, read_back_branches=3, read_back_tip_matches=2)]
        merged = aggregate.merge(rows, expected_workers=2)
        self.assertFalse(merged["complete"])
        self.assertEqual(merged["verification"]["failed_workers"], [1])
        self.assertEqual(merged["verification"]["advertised"], {"checked": 6, "tip_matches": 5})

    def test_split_checks_are_summed_apart(self):
        check = {"status": "ok", "advertised": {"status": "ok", "checked": 2, "tip_matches": 2},
                 "fetched": {"status": "ok", "checked": 2, "commits_present": 2}}
        merged = aggregate.merge([self.row(0, verification=check), self.row(1, verification=check)], expected_workers=2)
        self.assertTrue(merged["complete"])
        self.assertEqual(merged["verification"]["fetched"], {"checked": 4, "commits_present": 4})


class _HungHttp:
    """Stands in for aiohttp.ClientSession: every request is accepted and never answered
    (no response headers), like a server that holds the connection open forever."""

    def __init__(self):
        self.started = self.cancelled = 0

    def get(self, url, **kwargs):
        return self._hang()

    async def _hang(self):
        self.started += 1
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            # Closing takes a moment, like a real connection, so a caller that cancels without
            # awaiting the request returns before this runs; a caller that awaits it always sees it.
            await asyncio.sleep(.05)
            self.cancelled += 1
            raise


class MonitorDeadlineTests(unittest.IsolatedAsyncioTestCase):
    """Re-review 3: a server that never sends headers cannot hang the monitor past its window."""

    async def test_stream_header_wait_is_bounded(self):
        m = monitor.Monitor("http://127.0.0.1", 1, 1)
        m.header_timeout = .2
        http = _HungHttp()
        began = time.monotonic()
        await asyncio.wait_for(m.stream(http, time.time() + 1), 10)
        self.assertLess(time.monotonic() - began, 1 + 1)
        self.assertGreaterEqual(m.stream_connects["header_timeout"], 2)  # retried, never stuck
        self.assertEqual(http.cancelled, http.started)  # every hung request was cancelled

    async def test_observe_cancels_stuck_tasks_at_window_end(self):
        m = monitor.Monitor("http://127.0.0.1", .5, .1)
        m.grace_seconds = .3
        m.header_timeout = 3600  # even with no header bound, the window end must stop the stream

        poll_cancelled = []

        async def stuck_poll(http, end):
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                poll_cancelled.append(True)
                raise
        m.poll = stuck_poll
        http = _HungHttp()
        began = time.monotonic()
        summary = await asyncio.wait_for(m.observe(http), 10)
        self.assertLess(time.monotonic() - began, .5 + .3 + 1)
        # The stream's header wait is capped by the time left, so it ends by itself; the stuck poll
        # (no request bound at all) is what the window end has to cancel.
        self.assertEqual(summary["tasks_cancelled_at_window_end"], 1)
        self.assertEqual(poll_cancelled, [True])  # cancelled and finished before observe returned
        self.assertEqual(http.cancelled, http.started)


class MonitorTests(unittest.TestCase):
    def event(self, m, now, **event):
        return m.handle(None, json.dumps({"at": now, **event}), now=now)

    def test_resync_drops_pending_pushes_and_marks_lag_incomplete(self):
        m = monitor.Monitor("http://127.0.0.1", 10, 1)
        m.observe_start = 1000
        self.event(m, 1001, type="push", seq=5)
        self.assertEqual(m.last_seq, 5)
        self.assertTrue(self.event(m, 1002, type="resync", seq=0))
        self.assertIsNone(m.last_seq)  # reconnect from the live edge, not after=5
        self.assertEqual(m.unmerged, {})
        # After a restart the numbers start again: a merge through 7 covers the new push 1,
        # and must not also cover the old push 5 (which would add a 6 s lag).
        self.event(m, 1003, type="push", seq=1)
        self.event(m, 1004, type="merge", seq=2, throughSeq=7)
        self.assertEqual(m.lags, [1])
        self.assertEqual(m.dropped_by_resync, 1)
        lag = m.summary(1010)["merge_lag_seconds"]
        self.assertEqual((lag["complete"], lag["pushes_dropped_by_resync"], lag["pushes_merged"]), (False, 1, 1))

    def test_unbroken_window_reports_complete_lag(self):
        m = monitor.Monitor("http://127.0.0.1", 10, 1)
        m.observe_start = 1000
        self.event(m, 1001, type="push", seq=1)
        self.event(m, 1003, type="merge", seq=2, throughSeq=1)
        lag = m.summary(1010)["merge_lag_seconds"]
        self.assertEqual((lag["complete"], lag["p50"], lag["max"]), (True, 2, 2))

    def test_no_events_reports_the_whole_window_as_silence(self):
        m = monitor.Monitor("http://127.0.0.1", 60, 1)
        m.observe_start = 1000
        self.assertEqual(m.summary(1060)["max_seconds_between_events"], 60)

    def test_event_gap_counts_from_start_and_to_end(self):
        m = monitor.Monitor("http://127.0.0.1", 60, 1)
        m.observe_start = 1000
        self.assertEqual(m.gap_until(1060), 60)
        self.event(m, 1030, type="push", seq=1)
        self.event(m, 1031, type="push", seq=2)
        self.assertEqual(m.max_event_gap, 30)
        self.assertEqual(m.gap_until(1050), 30)
        self.assertEqual(m.gap_until(1075), 44)


if __name__ == "__main__":
    unittest.main()
