import asyncio
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import aggregate
import place_load as load


class ConfigTests(unittest.TestCase):
    def test_default_schedule_is_12_workers_at_50pps_with_five_ramp_steps(self):
        config = load.Config()
        config.validate()
        offers = list(load.offers(config))
        self.assertEqual(sum(phase == "ramp" for _, phase, _ in offers), 1800)
        self.assertEqual(sum(phase == "hold" for _, phase, _ in offers), 15000)
        self.assertEqual(len(offers), len({number for number, _, _ in offers}))
        self.assertEqual(config.workers * config.rate, 600)
        self.assertEqual(config.sessions, 110)
        for index, rate in enumerate((10, 20, 30, 40, 50)):
            times = [offset for _, phase, offset in offers if phase == "ramp" and index * 12 <= offset < (index + 1) * 12]
            self.assertEqual(len(times), rate * 12)
            self.assertAlmostEqual(times[1] - times[0], 1 / rate)
        first = next(load.offers(replace(config, worker=11)))
        self.assertAlmostEqual(first[2], 11 / 120)

    def test_default_session_reuse_stays_below_30_in_any_rolling_minute(self):
        config = load.Config()
        slots = [[] for _ in range(config.sessions)]
        for number, _, offset in load.offers(config):
            slots[number % config.sessions].append(offset)
        for times in slots:
            for start, offset in enumerate(times):
                self.assertLessEqual(sum(t < offset + 60 for t in times[start:]), 30)

    def test_no_network_without_execute_even_when_target_is_configured(self):
        with patch.dict("os.environ", {"ASPEN_LOAD_TARGET_URL": "https://example.invalid/api/v1"}), patch("asyncio.run") as run:
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(load.main([]), 0)
            self.assertTrue(json.loads(output.getvalue())["dry_run"])
            run.assert_not_called()

    def test_execute_requires_target_and_url_cannot_carry_secrets_or_redirect_path(self):
        for config in (load.Config(execute=True), load.Config(deadline_seconds=421),
                       load.Config(hold_seconds=400), load.Config(worker=12),
                       load.Config(ramp_seconds=float("nan")), load.Config(run="bad message")):
            with self.assertRaises(ValueError):
                config.validate()
        for target in ("https://user:password@example.invalid/api/v1", "https://example.invalid/api/v1?token=secret",
                       "https://example.invalid/api/v1#secret", "http://example.invalid/api/v1", "https://example.invalid/wrong"):
            with self.assertRaises(ValueError):
                load.Config(target_url=target).validate()
        load.Config(target_url="http://127.0.0.1:9876/api/v1").validate()
        load.Config(target_url="https://example.invalid/api/v1/").validate()

    def test_execute_above_500_requires_explicit_persona_acknowledgement(self):
        target = ["--target-url", "https://example.invalid/api/v1"]
        with patch.dict("os.environ", {}, clear=True):
            with (
                patch.object(load, "execute", new=unittest.mock.Mock(return_value=None)),
                patch("asyncio.run", return_value={"fatal": None}) as run,
                redirect_stdout(io.StringIO()),
                redirect_stderr(io.StringIO()) as errors,
            ):
                with self.assertRaises(SystemExit) as refused:
                    load.main(["--execute", *target])
                self.assertEqual(refused.exception.code, 2)
                self.assertIn("--acknowledge-persona-impact", errors.getvalue())
                run.assert_not_called()
            acknowledged = load.parse_args(["--execute", *target, "--acknowledge-persona-impact"])
            self.assertTrue(acknowledged.acknowledge_persona_impact)
            self.assertTrue(load.parse_args(["--execute", *target, "--workers", "10"]).execute)
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                load.parse_args(["--execute", *target, "--workers", "1", "--rate", "501"])
            self.assertFalse(load.parse_args(target).execute)
        with patch.dict("os.environ", {"ASPEN_LOAD_ACKNOWLEDGE_PERSONA_IMPACT": "true"}):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                load.parse_args(["--execute", *target])

    def test_environment_overrides_and_cli_wins(self):
        with patch.dict("os.environ", {"ASPEN_LOAD_RATE": "40", "ASPEN_LOAD_WORKER": "2"}):
            config = load.parse_args(["--rate", "50"])
        self.assertEqual((config.rate, config.worker), (50, 2))


def use_virtual_time(loop):
    """Make the loop's clock virtual: when nothing is ready it jumps to the next timer.

    Timings in these tests then depend only on the schedule, never on how busy the
    machine is (a CPU stall used to make setup miss a 30 ms start lead)."""
    clock = [0.0]
    loop.time = lambda: clock[0]
    select = loop._selector.select

    def virtual_select(timeout=None):
        if timeout:
            clock[0] += timeout
        return select(0)
    loop._selector.select = virtual_select


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.loop = asyncio.get_running_loop()
        use_virtual_time(self.loop)
        self.wall_base = 1_800_000_000.0

    def config(self, **kwargs):
        # start_at None: the runner picks its start from its own (virtual) wall clock.
        config = load.Config(target_url="http://127.0.0.1/api/v1", execute=True,
                             ramp_seconds=0, hold_seconds=.12, request_timeout=.05,
                             deadline_seconds=1, start_at=None)
        return replace(config, **kwargs)

    def vwall(self):
        return self.wall_base + self.loop.time()

    def runner(self, config, **kwargs):
        kwargs.setdefault("clock", self.loop.time)
        kwargs.setdefault("wall", self.vwall)
        return load.Runner(config, object(), **kwargs)

    def server(self, replies=None, delay=0):
        calls = []
        sessions = []

        async def post(http, url, body, token=None):
            if url.endswith("/sessions"):
                number = len(sessions)
                sessions.append(number)
                return 201, {"branch": f"visitors/{number}", "token": f"MEMORY_ONLY_{number}"}
            calls.append((body, token, time.monotonic()))
            await asyncio.sleep(delay)
            if replies:
                return replies[(len(calls) - 1) % len(replies)]
            return 201, {"commit": f"{len(calls):040x}", "seq": len(calls)}

        return post, calls, sessions

    def reconcile(self, result):
        for stats in result["phases"].values():
            self.assertEqual(stats["scheduled"], stats["sent"] + sum(stats["skipped"].values()))
            self.assertEqual(stats["sent"], stats["success"] + sum(stats["failures"].values()))
            self.assertEqual(stats["success"], sum(stats["latency_histogram_ms"].values()))

    async def test_one_durable_ack_per_request_no_retries_or_secret_logs(self):
        post, calls, sessions = self.server()
        with patch.object(load, "post_json", post):
            runner = self.runner(self.config())
            result = await runner.run()
        self.assertIsNone(result["fatal"])
        self.assertEqual(len(sessions), 110)
        self.assertEqual(len(calls), 6)
        self.assertEqual(result["phases"]["hold"]["success"], 6)
        self.assertTrue(all(len(body["tiles"]) == 1 for body, _, _ in calls))
        self.assertEqual(len({body["message"] for body, _, _ in calls}), 6)
        self.assertTrue(all(token.startswith("MEMORY_ONLY_") for _, token, _ in calls))
        self.assertNotIn("MEMORY_ONLY", json.dumps(result))
        self.assertNotIn("target_url", result["config"])
        self.assertEqual(runner.sessions, [])
        self.reconcile(result)

    async def test_refusals_invalid_acks_duplicates_and_unknown_outcomes_are_not_successes(self):
        replies = [(201, {"commit": "a" * 40, "seq": 1}),
                   (201, {"commit": "a" * 40, "seq": 2}),
                   (201, {"commit": "b" * 40, "seq": True}),
                   (200, {"commit": "c" * 40, "seq": 3}),
                   (429, {"message": "place is at its limit MEMORY_ONLY_0"}),
                   (429, {"message": "You can push 30 times a minute MEMORY_ONLY_0"})]
        post, calls, _ = self.server(replies)
        with patch.object(load, "post_json", post):
            result = await self.runner(self.config()).run()
        stats = result["phases"]["hold"]
        self.assertEqual(len(calls), 6)
        self.assertEqual(stats["success"], 1)
        self.assertEqual(stats["failures"], {"duplicate_commit": 1, "invalid_201_ack": 1,
                         "http_200": 1, "http_429:global_ceiling": 1, "http_429:session_quota": 1})
        self.assertNotIn("MEMORY_ONLY", json.dumps(result))
        self.reconcile(result)

    async def test_transport_errors_and_timeouts_are_not_retried(self):
        calls = 0

        async def post(http, url, body, token=None):
            nonlocal calls
            if url.endswith("/sessions"):
                return 201, {"branch": "visitors/test", "token": "MEMORY_ONLY"}
            calls += 1
            if calls % 2:
                raise TimeoutError("MEMORY_ONLY")
            raise RuntimeError("MEMORY_ONLY")

        with patch.object(load, "post_json", post):
            result = await self.runner(self.config()).run()
        self.assertEqual(calls, 6)
        self.assertEqual(result["phases"]["hold"]["failures"],
                         {"timeout_outcome_unknown": 3, "transport_error_outcome_unknown": 3})
        self.assertNotIn("MEMORY_ONLY", json.dumps(result))
        self.reconcile(result)

    async def test_distinct_python310_asyncio_and_builtin_timeouts_are_classified(self):
        class LegacyAsyncioTimeoutError(Exception):
            pass

        with patch.object(load.asyncio, "TimeoutError", LegacyAsyncioTimeoutError):
            for timeout in (TimeoutError, LegacyAsyncioTimeoutError):
                with self.subTest(timeout=timeout.__name__):
                    runner = self.runner(self.config())
                    runner.sessions = [("visitors/test", "MEMORY_ONLY")]
                    runner.active.add(0)
                    post = AsyncMock(side_effect=timeout("MEMORY_ONLY"))
                    with patch.object(load, "post_json", post):
                        await runner.push(0, "hold", 0)
                    self.assertEqual(runner.phases["hold"].failures, {"timeout_outcome_unknown": 1})
                    self.assertEqual(runner.phases["hold"].success, 0)
                    self.assertEqual(runner.active, set())
                    post.assert_awaited_once()
                    runner.work = AsyncMock(side_effect=timeout("MEMORY_ONLY"))
                    result = await runner.run()
                    self.assertEqual(result["fatal"], "deadline_exceeded")
                    self.assertNotIn("MEMORY_ONLY", json.dumps(result))
                    runner.work.assert_awaited_once()

    async def test_busy_sessions_and_inflight_limit_skip_without_backlog(self):
        for options, reason in (({"sessions": 1}, "session_busy"), ({"max_inflight": 1}, "inflight_limit")):
            post, calls, _ = self.server(delay=.2)
            with patch.object(load, "post_json", post):
                result = await self.runner(self.config(**options)).run()
            stats = result["phases"]["hold"]
            self.assertEqual(stats["scheduled"], 6)
            self.assertEqual(stats["sent"], 1)
            self.assertEqual(stats["skipped"], {reason: 5})
            self.assertEqual(len(calls), 1)
            self.reconcile(result)

    async def test_scheduler_lag_skips_offers_instead_of_catching_up(self):
        post, calls, _ = self.server()
        now = [10.0]

        async def late_sleep(seconds):
            now[0] += seconds + .01

        with patch.object(load, "post_json", post):
            runner = load.Runner(self.config(start_at=100.1, max_lag_seconds=.005), object(), clock=lambda: now[0],
                                 wall=lambda: 100, sleep=late_sleep)
            result = await runner.run()
        self.assertEqual(result["phases"]["hold"]["skipped"], {"scheduler_lag": 6})
        self.assertEqual(calls, [])
        self.reconcile(result)

    async def test_late_wakeup_never_sends_after_the_offering_window(self):
        post, calls, _ = self.server()
        now = [10.0]

        async def late_sleep(seconds):
            now[0] += seconds + 2

        with patch.object(load, "post_json", post):
            runner = load.Runner(self.config(start_at=100.1, max_lag_seconds=100), object(),
                                 clock=lambda: now[0], wall=lambda: 100, sleep=late_sleep)
            result = await runner.run()
        self.assertEqual(result["phases"]["hold"]["skipped"], {"window_ended": 6})
        self.assertEqual(calls, [])
        self.reconcile(result)

    async def test_deadline_cancels_drain_and_preserves_partial_counters(self):
        post, calls, _ = self.server(delay=5)
        with patch.object(load, "post_json", post):
            result = await self.runner(self.config(deadline_seconds=.25)).run()
        self.assertEqual(result["fatal"], "deadline_exceeded")
        self.assertLess(result["elapsed_seconds"], .5)
        self.assertEqual(result["phases"]["hold"]["failures"],
                         {"deadline_cancelled_outcome_unknown": len(calls)})
        self.reconcile(result)

    async def test_setup_refusal_and_impossible_start_send_no_pushes(self):
        for start, status, expected in ((self.vwall() + .03, 429, "setup_http_429:session_creation_quota"),
                                        (self.vwall() - 1, 201, "missed_common_start"),
                                        (self.vwall() + 2, 201, "schedule_and_drain_do_not_fit_deadline")):
            post = AsyncMock(return_value=(status, {"message": "Too many sessions MEMORY_ONLY"}))
            with patch.object(load, "post_json", post):
                result = await self.runner(self.config(start_at=start)).run()
            self.assertEqual(result["fatal"], expected)
            self.assertEqual(result["phases"]["hold"]["sent"], 0)
            self.assertNotIn("MEMORY_ONLY", json.dumps(result))
            self.reconcile(result)

    async def test_default_start_fits_the_absolute_deadline(self):
        post = AsyncMock(return_value=(429, {}))
        with patch.object(load, "post_json", post):
            result = await load.Runner(load.Config(target_url="https://example.invalid/api/v1"), object()).run()
        self.assertEqual(result["fatal"], "setup_http_429:other_rate_limit")

    async def test_http_transport_reads_fragmented_json_and_disables_redirects(self):
        response = AsyncMock()
        response.status = 201
        stream = asyncio.StreamReader()
        stream.feed_data(b'{"seq":')
        stream.feed_data(b'1}')
        stream.feed_eof()
        response.content = stream
        response.__aenter__.return_value = response
        http = unittest.mock.Mock()
        http.post.return_value = response
        self.assertEqual(await load.post_json(http, "https://example.invalid/api/v1/pushes", {}, "MEMORY_ONLY"),
                         (201, {"seq": 1}))
        self.assertFalse(http.post.call_args.kwargs["allow_redirects"])
        self.assertEqual(http.post.call_args.kwargs["headers"], {"Authorization": "Bearer MEMORY_ONLY"})

    async def test_refusals_record_whether_they_carry_retry_after(self):
        for headers, expected in (({"Retry-After": "1"}, "http_429:global_ceiling"),
                                  ({}, "http_429:no_retry_after:global_ceiling")):
            response = AsyncMock()
            response.status = 429
            response.headers = headers
            stream = asyncio.StreamReader()
            stream.feed_data(b'{"message":"place is at its limit"}')
            stream.feed_eof()
            response.content = stream
            response.__aenter__.return_value = response
            http = unittest.mock.Mock()
            http.post.return_value = response
            status, data = await load.post_json(http, "https://example.invalid/api/v1/pushes", {}, "T")
            self.assertEqual(load.refusal_reason(status, data), expected)
        self.assertEqual(load.refusal_reason(503, {"_retry_after": False}), "http_503:no_retry_after")


class AggregationTests(unittest.TestCase):
    def record(self, worker=0, histogram=None):
        hist = histogram or {"1": 1, "100": 1}
        success = sum(hist.values())
        return {"worker": worker, "run": "test", "start": 123,
                "config": {"workers": 2, "rate": 50, "ramp_seconds": 60, "hold_seconds": 300},
                "scheduling_finished": True, "fatal": None,
                "phases": {phase: {"scheduled": success + 3, "sent": success + 1,
                                   "success": success, "skipped": {"session_busy": 2},
                                   "failures": {"http_429:global_ceiling": 1},
                                   "latency_histogram_ms": hist} for phase in load.PHASES}}

    def test_merge_histograms_not_percentiles_and_deduplicate_exported_results(self):
        one, two = self.record(), self.record(1, {"5": 8})
        result = aggregate.merge([one, two, one], 2)
        self.assertTrue(result["complete"])
        self.assertEqual(result["phases"]["hold"]["success_latency_ms"], {"p50": 5, "p95": 100, "p99": 100})
        self.assertEqual(result["phases"]["hold"]["success"], 10)
        self.assertEqual(result["phases"]["hold"]["skipped"], {"session_busy": 4})
        self.assertEqual(result["phases"]["hold"]["failures"], {"http_429:global_ceiling": 2})
        self.assertEqual(result["success_latency_ms"]["p50"], 5)

    def test_missing_failed_and_unfinished_workers_are_explicit(self):
        one = self.record()
        result = aggregate.merge([one], 2)
        self.assertFalse(result["complete"])
        self.assertEqual(result["missing_workers"], [1])
        one.update(fatal="deadline_exceeded", scheduling_finished=False)
        result = aggregate.merge([one, self.record(1)], 2)
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_workers"], [0])
        self.assertEqual(result["unfinished_workers"], [0])

    def test_inconsistent_or_mixed_results_fail_closed(self):
        for change in ({"run": "other"}, {"start": 124}, {"worker": 3}):
            one = self.record(1)
            one.update(change)
            with self.assertRaises(ValueError):
                aggregate.merge([self.record(), one], 2)
        bad = self.record()
        bad["phases"]["hold"]["success"] += 1
        with self.assertRaises(ValueError):
            aggregate.merge([bad], 2)
        with self.assertRaises(ValueError):
            aggregate.merge([self.record(), self.record(histogram={"4": 2})], 2)

    def test_negative_counters_are_not_valid_evidence(self):
        record = self.record()
        record["phases"]["hold"]["skipped"] = {"session_busy": -1, "inflight_limit": 3}
        with self.assertRaises(ValueError):
            aggregate.merge([record], 2)

    def test_cloud_export_and_jsonl_progress_lines(self):
        record = self.record()
        cloud = {"events": [{"message": json.dumps({"result": record})}]}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "cloud.json"
            path.write_text(json.dumps(cloud))
            other = Path(folder) / "worker.jsonl"
            other.write_text('not json\n{"progress": 1}\n' + json.dumps({"result": self.record(1)}) + '\n')
            result = aggregate.merge(aggregate.load_results([path, other]), 2)
        self.assertTrue(result["complete"])

    def test_scratch_results_supported_without_echoing_raw_server_messages(self):
        legacy = {"worker": 0, "run": "legacy", "start": 123, "ramp_seconds": 60, "hold_seconds": 300,
                  "counts": {"hold_scheduled": 5, "hold_sent": 4, "hold_skipped_client": 1,
                             "hold_success": 2, "hold_status_429": 1, "hold_exception_TimeoutError": 1},
                  "reasons": {"hold:429:rate-limited:MEMORY_ONLY": 1},
                  "latency_histogram_ms": {"ramp": {}, "hold": {"3": 1, "7": 1}}}
        result = aggregate.merge([legacy], 1)
        self.assertTrue(result["complete"])
        self.assertEqual(result["phases"]["hold"]["success"], 2)
        self.assertEqual(result["phases"]["hold"]["failures"],
                         {"http_429:other_rate_limit": 1, "legacy_transport_error_outcome_unknown": 1})
        self.assertNotIn("MEMORY_ONLY", json.dumps(result))
        legacy["counts"]["hold_duplicate_commit"] = 1
        with self.assertRaises(ValueError):
            aggregate.merge([legacy], 1)


if __name__ == "__main__":
    unittest.main()
