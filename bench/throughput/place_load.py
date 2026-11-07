#!/usr/bin/env python3
"""Bounded, open-loop Place API load. No network activity without --execute."""

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
import ipaddress
import json
import math
import os
import re
import time
from urllib.parse import urlsplit

PHASES = ("ramp", "hold")
PALETTE = ("#d6262b", "#3690ea", "#ffd635", "#00a368")


@dataclass(frozen=True)
class Config:
    target_url: str = ""
    execute: bool = False
    acknowledge_persona_impact: bool = False
    worker: int = 0
    workers: int = 12
    rate: int = 50
    sessions: int = 110
    ramp_seconds: float = 60
    hold_seconds: float = 300
    deadline_seconds: float = 420
    request_timeout: float = 30
    start_at: float | None = None
    run: str = "aspen-load"
    max_inflight: int = 128
    max_lag_seconds: float = 1
    use_env_proxy: bool = False

    def validate(self):
        if not 0 <= self.worker < self.workers <= 128:
            raise ValueError("worker must be within workers (at most 128)")
        if not 1 <= self.rate <= 1000 or not 1 <= self.sessions <= 10000:
            raise ValueError("rate must be 1..1000 and sessions 1..10000")
        numbers = (self.ramp_seconds, self.hold_seconds, self.deadline_seconds,
                   self.request_timeout, self.max_lag_seconds)
        if any(not math.isfinite(n) for n in numbers):
            raise ValueError("durations must be finite")
        if (self.ramp_seconds < 0 or self.hold_seconds <= 0
                or not 0 < self.deadline_seconds <= 420
                or not 0 < self.request_timeout <= 30
                or self.max_lag_seconds < 0 or not 1 <= self.max_inflight <= 1024):
            raise ValueError("invalid durations or inflight limit; deadline is at most 420s")
        if self.ramp_seconds + self.hold_seconds + self.request_timeout >= self.deadline_seconds:
            raise ValueError("deadline must leave time for setup, schedule and request drain")
        if self.start_at is not None and not math.isfinite(self.start_at):
            raise ValueError("start-at must be a finite Unix timestamp")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", self.run):
            raise ValueError("run must contain 1..80 letters, digits, hyphens or underscores")
        if self.execute and self.rate * self.workers > 500 and not self.acknowledge_persona_impact:
            raise ValueError("execution above 500 total offers/s requires --acknowledge-persona-impact; personas may pause")
        if self.execute and not self.target_url:
            raise ValueError("--execute requires --target-url (or ASPEN_LOAD_TARGET_URL)")
        if self.target_url:
            url = urlsplit(self.target_url)
            try:
                loopback = ipaddress.ip_address(url.hostname or "").is_loopback
            except ValueError:
                loopback = url.hostname == "localhost"
            if (url.scheme not in ("https", "http") or not url.hostname
                    or url.username or url.password or url.query or url.fragment
                    or url.path.rstrip("/") != "/api/v1"
                    or (url.scheme == "http" and not loopback)):
                raise ValueError("target must be HTTPS /api/v1 without credentials/query; HTTP only on loopback")

    def public(self):
        return {key: value for key, value in vars(self).items() if key != "target_url"}


def offers(config):
    number = 0
    if config.ramp_seconds:
        segment_seconds = config.ramp_seconds / 5
        for segment in range(5):
            rate = config.rate * (segment + 1) / 5
            for tick in range(math.ceil(rate * segment_seconds)):
                offset = segment * segment_seconds + tick / rate + config.worker / (config.workers * rate)
                if offset < (segment + 1) * segment_seconds:
                    yield number, "ramp", offset
                    number += 1
    for tick in range(math.ceil(config.rate * config.hold_seconds)):
        offset = tick / config.rate + config.worker / (config.workers * config.rate)
        if offset < config.hold_seconds:
            yield number, "hold", config.ramp_seconds + offset
            number += 1


@dataclass
class Phase:
    scheduled: int = 0
    sent: int = 0
    skipped: Counter = field(default_factory=Counter)
    success: int = 0
    failures: Counter = field(default_factory=Counter)
    latency_histogram_ms: Counter = field(default_factory=Counter)

    def result(self):
        return {"scheduled": self.scheduled, "sent": self.sent,
                "skipped": dict(self.skipped), "success": self.success,
                "failures": dict(self.failures),
                "latency_histogram_ms": dict(self.latency_histogram_ms)}


def refusal_reason(status, data):
    message = data.get("message", "") if isinstance(data, dict) else ""
    if not isinstance(message, str):
        message = ""
    reason = "http_" + str(status)
    if status in (429, 503) and isinstance(data, dict) and data.get("_retry_after") is False:
        reason += ":no_retry_after"
    if status == 429:
        if "place is at its limit" in message:
            return reason + ":global_ceiling"
        if "You can push" in message:
            return reason + ":session_quota"
        if "Too many sessions" in message:
            return reason + ":session_creation_quota"
        return reason + ":other_rate_limit"
    return reason


async def post_json(http, url, body, token=None):
    headers = {"Authorization": "Bearer " + token} if token else {}
    async with http.post(url, json=body, headers=headers, allow_redirects=False) as response:
        try:
            raw = await response.content.readexactly(65537)
        except asyncio.IncompleteReadError as error:
            raw = error.partial
        data = None
        if len(raw) <= 65536:
            try:
                data = json.loads(raw)
            except (ValueError, UnicodeError):
                data = None
        if response.status in (429, 503):
            # Refusals above the ceiling must say when to retry; record whether this one did.
            data = data if isinstance(data, dict) else {}
            data["_retry_after"] = response.headers.get("Retry-After") is not None
        return response.status, data


class Runner:
    def __init__(self, config, http, clock=time.monotonic, wall=time.time, sleep=asyncio.sleep):
        self.config, self.http = config, http
        self.clock, self.wall, self.sleep = clock, wall, sleep
        self.phases = {name: Phase() for name in PHASES}
        self.sessions = []
        self.active, self.pending, self.commits = set(), set(), set()
        self.max_lag_ms = 0
        self.fatal = None
        self.start_at = None
        self.scheduling_finished = False

    async def push(self, number, phase, slot):
        stats = self.phases[phase]
        branch, token = self.sessions[slot]
        before = self.clock()
        try:
            body = {"branch": branch, "tiles": [{"x": (number + self.config.worker * 3) % 16,
                    "y": (number // 16 + self.config.worker * 5) % 16,
                    "color": PALETTE[(number // self.config.sessions + self.config.worker) % 4]}],
                    "message": f"{self.config.run}:{self.config.worker}:{number}"}
            status, data = await post_json(self.http, self.config.target_url.rstrip("/") + "/pushes", body, token)
            if status != 201:
                stats.failures[refusal_reason(status, data)] += 1
            elif (not isinstance(data, dict)
                  or not isinstance(data.get("commit"), str)
                  or not re.fullmatch(r"[0-9a-f]{40}", data["commit"])
                  or type(data.get("seq")) is not int or data["seq"] <= 0):
                stats.failures["invalid_201_ack"] += 1
            elif data["commit"] in self.commits:
                stats.failures["duplicate_commit"] += 1
            else:
                self.commits.add(data["commit"])
                stats.success += 1
                stats.latency_histogram_ms[max(1, math.ceil((self.clock() - before) * 1000))] += 1
        except asyncio.CancelledError:
            stats.failures["deadline_cancelled_outcome_unknown"] += 1
            raise
        except (TimeoutError, asyncio.TimeoutError):
            stats.failures["timeout_outcome_unknown"] += 1
        except Exception:
            stats.failures["transport_error_outcome_unknown"] += 1
        finally:
            self.active.discard(slot)

    async def work(self, deadline):
        config = self.config
        self.start_at = config.start_at if config.start_at is not None else self.wall() + min(
            20, (config.deadline_seconds - config.ramp_seconds - config.hold_seconds - config.request_timeout) / 2
        )
        start = self.clock() + self.start_at - self.wall()
        if start + config.ramp_seconds + config.hold_seconds + config.request_timeout > deadline:
            self.fatal = "schedule_and_drain_do_not_fit_deadline"
            return
        if start <= self.clock():
            self.fatal = "missed_common_start"
            return
        for _ in range(config.sessions):
            if self.clock() >= start:
                self.fatal = "setup_missed_common_start"
                return
            status, data = await post_json(self.http, config.target_url.rstrip("/") + "/sessions", {})
            if status != 201:
                self.fatal = "setup_" + refusal_reason(status, data)
                return
            if (not isinstance(data, dict) or not isinstance(data.get("branch"), str)
                    or not isinstance(data.get("token"), str) or not data["branch"] or not data["token"]):
                self.fatal = "invalid_session_ack"
                return
            self.sessions.append((data["branch"], data["token"]))
        if self.clock() >= start:
            self.fatal = "setup_missed_common_start"
            return
        for number, phase, offset in offers(config):
            due = start + offset
            await self.sleep(max(0, due - self.clock()))
            stats = self.phases[phase]
            stats.scheduled += 1
            lag = self.clock() - due
            self.max_lag_ms = max(self.max_lag_ms, math.ceil(lag * 1000))
            slot = number % config.sessions
            if self.clock() >= start + config.ramp_seconds + config.hold_seconds:
                stats.skipped["window_ended"] += 1
            elif lag > config.max_lag_seconds:
                stats.skipped["scheduler_lag"] += 1
            elif slot in self.active:
                stats.skipped["session_busy"] += 1
            elif len(self.pending) >= config.max_inflight:
                stats.skipped["inflight_limit"] += 1
            else:
                self.active.add(slot)
                stats.sent += 1
                task = asyncio.create_task(self.push(number, phase, slot))
                self.pending.add(task)
                task.add_done_callback(self.pending.discard)
        self.scheduling_finished = True
        if self.pending:
            await asyncio.gather(*list(self.pending))

    async def run(self):
        began = self.clock()
        try:
            await asyncio.wait_for(self.work(began + self.config.deadline_seconds), self.config.deadline_seconds)
        except (TimeoutError, asyncio.TimeoutError):
            self.fatal = "deadline_exceeded"
        except Exception:
            self.fatal = "setup_transport_error"
        finally:
            pending = list(self.pending)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self.sessions.clear()
        return {"schema": "aspen-place-load-v1", "worker": self.config.worker, "run": self.config.run,
                "config": self.config.public(), "start": self.start_at,
                "elapsed_seconds": round(self.clock() - began, 3), "fatal": self.fatal,
                "scheduling_finished": self.scheduling_finished,
                "max_schedule_lag_ms": self.max_lag_ms,
                "unique_successful_commits": len(self.commits),
                "phases": {name: phase.result() for name, phase in self.phases.items()}}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name, kind, default in (("target-url", str, ""), ("worker", int, 0), ("workers", int, 12),
                                ("rate", int, 50), ("sessions", int, 110), ("ramp-seconds", float, 60),
                                ("hold-seconds", float, 300), ("deadline-seconds", float, 420),
                                ("request-timeout", float, 30), ("start-at", float, None),
                                ("run", str, "aspen-load"), ("max-inflight", int, 128),
                                ("max-lag-seconds", float, 1)):
        parser.add_argument("--" + name, type=kind, default=os.getenv("ASPEN_LOAD_" + name.upper().replace("-", "_"), default))
    parser.add_argument("--execute", action="store_true", help="send traffic; never enabled from the environment")
    parser.add_argument("--use-env-proxy", action="store_true", help="honour HTTPS_PROXY (sandboxed control hosts)")
    parser.add_argument("--acknowledge-persona-impact", action="store_true",
                        help="acknowledge that execution above 500 total offers/s may pause personas; not authorization")
    args = parser.parse_args(argv)
    config = Config(**vars(args))
    try:
        config.validate()
    except ValueError as error:
        parser.error(str(error))
    return config


async def execute(config):
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=config.request_timeout, connect=min(5, config.request_timeout), ceil_threshold=1000)
    connector = aiohttp.TCPConnector(limit=config.max_inflight)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=config.use_env_proxy) as http:
        return await Runner(config, http).run()


def main(argv=None):
    config = parse_args(argv)
    if not config.execute:
        counts = Counter(phase for _, phase, _ in offers(config))
        print(json.dumps({"dry_run": True, "config": config.public(), "scheduled_per_worker": dict(counts),
                          "total_hold_offers_per_second": config.rate * config.workers}))
        return 0
    try:
        result = asyncio.run(execute(config))
    except ImportError:
        print(json.dumps({"fatal": "install_bench_throughput_requirements"}))
        return 2
    print(json.dumps({"result": result}, separators=(",", ":")), flush=True)
    return 2 if result["fatal"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
