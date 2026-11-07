#!/usr/bin/env python3
"""Mutation check for the item 10 bench fixes: remove each fix in a scratch copy and show a test fails.

Run from anywhere: python3 bench/throughput/tools/mutants.py. Prints one row per mutant
(killed means at least one named test failed with the fix removed). Exits 1 if any survives.
Only touches a temporary copy of bench/throughput; needs git and Python 3.11, no network.
"""
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
L, G, M = "test_launch_ec2.LaunchTest.", "test_git_load_monitor.GitLoadRunnerTests.", "test_git_load_monitor.MonitorTests."
C = "test_git_load_monitor.GitLoadConfigTests."
S = "test_git_load_monitor.SubprocessDeadlineTests."
M2 = "test_git_load_monitor.MonitorDeadlineTests."

# (id, file, old, new, tests)
MUTANTS = [
    ("1 evidence before terminate", "launch_ec2.py",
     '    ids = set(state.get("instance_ids", []))\n',
     '    _download_evidence(s3, state, out_dir) if _bucket_present(s3, state) else None\n'
     '    ids = set(state.get("instance_ids", []))\n',
     [L + "test_evidence_access_denied_still_terminates_and_keeps_bucket",
      L + "test_local_write_failure_still_terminates", L + "test_rollback_survives_evidence_failure_and_reraises_original"]),
    ("1 abort on first failure", "launch_ec2.py",
     "            failures.append((step, exc))\n            return default",
     "            raise",
     [L + "test_terminate_failure_does_not_stop_later_steps", L + "test_evidence_listing_unavailable_still_terminates"]),
    ("1 bucket deleted despite failed evidence", "launch_ec2.py",
     "if evidence_ok or discard_evidence:", "if True:",
     [L + "test_evidence_access_denied_still_terminates_and_keeps_bucket"]),
    ("1 watch without finally", "launch_ec2.py",
     "    finally:\n        teardown(clients, state, state_path, sleep=sleep)\n    return reason",
     "    except BaseException:\n        raise\n    teardown(clients, state, state_path, sleep=sleep)\n    return reason",
     [L + "test_watch_tears_down_when_polling_fails"]),
    ("2 first page only", "launch_ec2.py",
     '        kwargs["NextToken"] = token', '        return items',
     [L + "test_teardown_finds_tagged_instance_and_group_on_a_later_page", L + "test_wait_terminated_reads_every_page",
      L + "test_reap_reads_every_page"]),
    ("3 state file per run name", "launch_ec2.py",
     'f"{run}-{nonce}.json"', 'f"{run}.json"', [L + "test_reused_run_name_gets_its_own_state_file"]),
    ("3 state file overwritten", "launch_ec2.py",
     "os.O_WRONLY | os.O_CREAT | os.O_EXCL", "os.O_WRONLY | os.O_CREAT",
     [L + "test_existing_state_file_is_refused_not_overwritten"]),
    ("4 no tag discovery of groups", "launch_ec2.py",
     '    sgs |= set(attempt("discover_security_groups", lambda: _tagged_security_groups(ec2, state), []))\n', '',
     [L + "test_security_group_created_but_not_recorded_is_found_by_tags",
      L + "test_standalone_teardown_after_crash_finds_unrecorded_resources"]),
    ("4 bucket status saved after create", "launch_ec2.py",
     '    state["bucket_status"] = "creating"\n    save_state(state_path, state)\n', '',
     [L + "test_bucket_created_but_not_recorded_is_found_by_name"]),
    ("4 refused bucket touched", "launch_ec2.py",
     'not in ("creating", "created"):', 'not in ("creating", "created", "refused"):',
     [L + "test_refused_bucket_is_never_touched_by_teardown"]),
    ("5 retired lanes dropped from checks", "git_load.py",
     "        expected = dict(self.landed)\n",
     "        expected = {b: c for b, c in self.landed.items() if b not in retired}\n",
     [G + "test_retired_lane_whose_branch_vanished_fails_verification",
      G + "test_fresh_fetch_catches_an_acknowledged_commit_gone_from_a_retired_branch"]),
    ("5 missing retired branch accepted", "git_load.py",
     "elif tip is not None and branch in retired:", "elif branch in retired:",
     [G + "test_retired_lane_whose_branch_vanished_fails_verification"]),
    ("7 fabricated fresh-fetch success", "git_load.py",
     '            return {"status": "ok" if present == len(expected) else "mismatch",',
     '            present = len(expected)\n            return {"status": "ok" if present == len(expected) else "mismatch",',
     [G + "test_fresh_fetch_catches_an_acknowledged_commit_gone_from_a_retired_branch"]),
    ("6 verification outside the deadline", "git_load.py",
     "await asyncio.wait_for(self.verify(), max(0, deadline - time.monotonic()))", "await self.verify()",
     [G + "test_verification_shares_the_run_deadline_and_kills_a_hung_fetch"]),
    ("6 hung child not killed", "git_load.py",
     "    except BaseException:\n        kill_group(process)\n        cancelled = False\n",
     "    except BaseException:\n        cancelled = False\n",
     [G + "test_verification_shares_the_run_deadline_and_kills_a_hung_fetch"]),
    ("7 definite refusal keeps stale index", "git_load.py",
     "        if lane.tip != commit:", "        if lane.tip != commit and lane.retired:",
     [G + "test_definite_refusal_keeps_lane_and_restores_index"]),
    ("7 resync keeps pending pushes", "monitor.py", "            self.unmerged.clear()\n", "",
     [M + "test_resync_drops_pending_pushes_and_marks_lag_incomplete"]),
    ("7 resync reconnects after old seq", "monitor.py", "            self.last_seq = None\n", "",
     [M + "test_resync_drops_pending_pushes_and_marks_lag_incomplete"]),
    ("7 lag always complete", "monitor.py", '"complete": not self.resyncs', '"complete": True',
     [M + "test_resync_drops_pending_pushes_and_marks_lag_incomplete"]),
    ("6 gap ignores start", "monitor.py",
     "self.last_event_wall if self.last_event_wall is not None else self.observe_start\n        if previous",
     "self.last_event_wall\n        if previous", [M + "test_event_gap_counts_from_start_and_to_end"]),
    ("6 gap ignores end", "monitor.py", "return max(self.max_event_gap, end - previous)",
     "return self.max_event_gap", [M + "test_no_events_reports_the_whole_window_as_silence",
                                   M + "test_event_gap_counts_from_start_and_to_end"]),
    # re-review 2
    ("R2-1 bucket deleted on account ownership alone", "launch_ec2.py",
     "    if any(have.get(k) != v for k, v in want.items()):", "    if False:",
     [L + "test_name_taken_after_head_with_ambiguous_create_error_is_not_deleted",
      L + "test_refused_create_whose_marker_was_not_saved_is_not_deleted",
      L + "test_bucket_with_another_runs_nonce_is_not_deleted",
      L + "test_crash_between_create_and_tagging_leaves_untagged_bucket_reported"]),
    ("R2-1 bucket tagged after it is marked created", "launch_ec2.py",
     '    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": run_tags(state)})\n'
     '    state["bucket_status"] = "created"\n    save_state(state_path, state)\n',
     # only the save moves ahead of tagging; no other call is added or reordered
     '    state["bucket_status"] = "created"\n    save_state(state_path, state)\n'
     '    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": run_tags(state)})\n',
     [L + "test_bucket_is_tagged_before_it_is_marked_created"]),
    ("R2-2 descendants not killed (direct child only)", "git_load.py",
     "        os.killpg(process.pid, signal.SIGKILL)", "        process.kill()",
     [S + "test_timeout_kills_pipe_inheriting_descendants", S + "test_hung_setup_clone_is_killed_at_the_deadline"]),
    ("R2-2 setup clone bypasses run_process", "git_load.py",
     '        code, _, _ = await run_process(\n            ("git", "-c", "protocol.version=2", "clone"',
     '        async def bare(argv, timeout=None):  # the old unbounded clone: no deadline, no kill\n'
     '            process = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.DEVNULL,\n'
     '                                                           stderr=asyncio.subprocess.PIPE)\n'
     '            await process.communicate()\n'
     '            return process.returncode, "", ""\n'
     '        code, _, _ = await bare(\n            ("git", "-c", "protocol.version=2", "clone"',
     [S + "test_hung_setup_clone_is_killed_at_the_deadline"]),
    ("R2-2 restore shielded and unbounded", "git_load.py",
     "                await self.restore_index(lane)\n",
     "                await asyncio.shield(self.restore_index(lane))\n",
     [S + "test_index_restore_is_bounded_by_the_deadline"]),
    ("R2-3 evidence path without nonce", "launch_ec2.py",
     'f"{state[\'run\']}-{state[\'nonce\']}", rel)', 'state["run"], rel)',
     [L + "test_reused_run_name_keeps_both_runs_evidence"]),
    ("R2-3 evidence overwritten", "launch_ec2.py",
     "                    if f.read() != body:", "                    if False:",
     [L + "test_evidence_never_overwrites_different_bytes_but_reruns_cleanly"]),
    ("R2-4 vanished recorded id blocks termination", "launch_ec2.py",
     '        if error_code(exc) not in ("InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"):\n            raise\n',
     "        raise\n",
     [L + "test_vanished_recorded_instance_does_not_block_live_orphan"]),
    # re-review 3
    ("R3-1 us-east-1 allowed (create adopts an owned bucket)", "launch_ec2.py",
     '    if region == "us-east-1":\n', '    if False:\n',
     [L + "test_us_east_1_is_refused_before_any_write"]),
    ("R3-1 us-east-1 create path restored", "launch_ec2.py",
     '        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": state["region"]})',
     '        s3.create_bucket(Bucket=bucket) if state["region"] == "us-east-1" else '
     's3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": state["region"]})',
     [L + "test_create_never_uses_the_us_east_1_form"]),
    ("R3-1 default region us-east-1", "launch_ec2.py",
     'p.add_argument("--region", default="us-east-2")', 'p.add_argument("--region", default="us-east-1")',
     [L + "test_default_region_is_not_us_east_1"]),
    ("R3-1 owned-bucket refusal ignored", "launch_ec2.py",
     'if error_code(exc) in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):',
     'if error_code(exc) in ("BucketAlreadyExists",):',
     [L + "test_bucket_created_after_absence_check_is_refused_not_adopted"]),
    ("R4-1 cleanup gives up on a repeated cancel", "git_load.py",
     "            except asyncio.CancelledError:\n                cancelled = True\n",
     "            except asyncio.CancelledError:\n                raise\n",
     [S + "test_repeated_cancellation_during_cleanup_still_kills_and_reaps"]),
    ("R4-2 deadline starts after the spawn", "git_load.py",
     "    deadline = None if timeout is None else loop.time() + timeout\n    process = subprocess.Popen(\n"
     "        argv, env=env, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,\n"
     "        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)\n",
     "    process = subprocess.Popen(\n"
     "        argv, env=env, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,\n"
     "        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)\n"
     "    deadline = None if timeout is None else loop.time() + timeout\n",
     [S + "test_slow_spawn_spends_the_deadline"]),
    ("R3-3 stream header wait unbounded", "monitor.py",
     "                    response = await asyncio.wait_for(asyncio.shield(connect), min(self.header_timeout,\n"
     "                                                                                   max(0.1, end - time.time())))\n",
     "                    response = await connect\n",
     [M2 + "test_stream_header_wait_is_bounded"]),
    ("R3-3 timed-out header request not awaited after cancel", "monitor.py",
     "                    connect.cancel()\n                    await asyncio.gather(connect, return_exceptions=True)\n",
     "                    connect.cancel()\n",
     [M2 + "test_stream_header_wait_is_bounded"]),
    ("R3-3 observe gathers without a deadline", "monitor.py",
     "            _, late = await asyncio.wait(tasks, timeout=self.seconds + self.grace_seconds)\n",
     "            _, late = await asyncio.wait(tasks)\n",
     [M2 + "test_observe_cancels_stuck_tasks_at_window_end"]),
    ("R3-3 stuck tasks not cancelled at window end", "monitor.py",
     "            for task in tasks:\n                task.cancel()\n            await asyncio.gather(*tasks, return_exceptions=True)\n",
     "            pass\n",
     [M2 + "test_observe_cancels_stuck_tasks_at_window_end"]),
]
GUARDS = [  # (old, new) in git_load.Config.validate; each must be caught by the boundary tests
    ("if any(not math.isfinite(n) for n in numbers):", "if False:"),
    ("self.ramp_seconds < 0 or", "self.ramp_seconds < -1 or"),
    ("self.hold_seconds <= 0", "self.hold_seconds < 0"),
    ("not 0 < self.deadline_seconds <= 900", "not 0 < self.deadline_seconds <= 901"),
    # Not listed: "0 < deadline" -> "0 <= deadline" is equivalent, because the schedule check
    # (ramp + hold + timeout >= deadline, hold and timeout > 0) already refuses any deadline <= 0.
    ("not 0 < self.request_timeout <= 120", "not 0 < self.request_timeout <= 121"),
    ("not 0 < self.request_timeout <= 120", "not 0 <= self.request_timeout <= 120"),
    ("self.max_lag_seconds < 0", "self.max_lag_seconds < -1"),
    ("not 1 <= self.max_inflight <= 1024", "not 0 <= self.max_inflight <= 1024"),
    ("not 1 <= self.max_inflight <= 1024", "not 1 <= self.max_inflight <= 1025"),
    ("if self.start_at is not None and not math.isfinite(self.start_at):", "if False:"),
    ("not 0 <= self.total_offered <= 2000", "not -1 <= self.total_offered <= 2000"),
    ("not 0 <= self.total_offered <= 2000", "not 0 <= self.total_offered <= 2001"),
    ("not 0 <= self.worker < self.workers <= 64", "not -1 <= self.worker < self.workers <= 64"),
    ("not 0 <= self.worker < self.workers <= 64", "not 0 <= self.worker <= self.workers <= 64"),
    ("not 0 <= self.worker < self.workers <= 64", "not 0 <= self.worker < self.workers <= 65"),
    ("not 1 <= self.rate <= 200", "not 0 <= self.rate <= 200"),
    ("not 1 <= self.rate <= 200", "not 1 <= self.rate <= 201"),
    ("not 1 <= self.lanes <= 5000", "not 0 <= self.lanes <= 5000"),
    ("not 1 <= self.lanes <= 5000", "not 1 <= self.lanes <= 5001"),
    ("self.rate * 60 / self.lanes > 30", "self.rate * 60 / self.lanes > 31"),
    ("self.request_timeout >= self.deadline_seconds", "self.request_timeout > self.deadline_seconds"),
]
MUTANTS += [(f"7 guard: {old} -> {new}", "git_load.py", old, new,
             [C + "test_each_numeric_guard_at_its_boundary", C + "test_rate_worker_quota_and_schedule_guards"])
            for old, new in GUARDS]


def main():
    survived = 0
    for name, path, old, new, tests in MUTANTS:
        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "throughput")
            shutil.copytree(HERE, work, ignore=shutil.ignore_patterns("results", "__pycache__"))
            source = open(os.path.join(work, path)).read()
            if source.count(old) < 1:
                print(f"| {name} | NOT APPLIED (pattern missing) |", flush=True)
                survived += 1
                continue
            open(os.path.join(work, path), "w").write(source.replace(old, new, 1))
            try:  # a mutant that hangs the tests (e.g. a leaked child holding a pipe) counts as killed
                run = subprocess.run([sys.executable, "-m", "unittest", *tests], cwd=work, capture_output=True,
                                     text=True, timeout=120, start_new_session=True)
                killed = run.returncode != 0
            except subprocess.TimeoutExpired:
                killed = True
                subprocess.run(["pkill", "-f", work], capture_output=True)
            survived += not killed
            print(f"| {name} | {'killed' if killed else 'SURVIVED'} |", flush=True)
    print(f"{len(MUTANTS) - survived}/{len(MUTANTS)} killed")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
