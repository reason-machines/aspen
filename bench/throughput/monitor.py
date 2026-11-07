#!/usr/bin/env python3
"""Watch a place deployment during a load run: page and API liveness, the live feed, merge lag.

Polls the page (`/`), `GET /api/v1/canvas` and `GET /api/v1/canvas.svg` every
`--interval` seconds and keeps one `GET /api/v1/events` stream open,
reconnecting with the last `seq`. Merge lag is measured from the stream's own
server timestamps: for every merge event, each push event with a `seq` at or
below its `throughSeq` that no earlier merge covered gets lag = merge `at` -
push `at`. A `resync` event (the server no longer has the events after our
`seq`, or restarted and numbers them again) drops every push still waiting for
a merge, since the events in between are lost, and marks merge lag incomplete;
samples are only ever taken between a push and a merge seen in one unbroken
window. The longest silence on the stream counts from the start and to the end
of observation, so a run with no events reports the whole window. Prints one
JSON summary at the end; tokens are never needed.
"""

import argparse
import asyncio
from collections import Counter
from datetime import datetime
import json
import math
import time


def parse_at(value):
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e11 else value
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def pct(values, q):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]


class Monitor:
    def __init__(self, base, seconds, interval, trust_env=False):
        self.trust_env = trust_env
        self.base, self.seconds, self.interval = base.rstrip("/"), seconds, interval
        self.polls = {name: Counter() for name in ("page", "canvas", "canvas_svg")}
        self.poll_ms = {name: [] for name in self.polls}
        self.unhealthy = []
        self.events = Counter()
        self.stream_connects = Counter()
        self.unmerged = {}
        self.lags = []
        self.last_seq = None
        self.max_event_gap = 0.0
        self.observe_start = None
        self.last_event_wall = None
        self.resyncs = []
        self.dropped_by_resync = 0
        self.timeline = []
        self.header_timeout = 15.0  # seconds to connect and receive the event stream's headers
        self.grace_seconds = 5.0  # after the window, still-running work is cancelled
        self.cancelled_at_window_end = 0
        self.stream_timeout = lambda: None

    async def poll(self, http, end):
        paths = {"page": "/", "canvas": "/api/v1/canvas", "canvas_svg": "/api/v1/canvas.svg"}
        while time.time() < end:
            tick = time.time()
            for name, path in paths.items():
                before = time.monotonic()
                try:
                    async with http.get(self.base + path, allow_redirects=False) as response:
                        await response.read()
                        status = str(response.status)
                except Exception:
                    status = "error"
                self.polls[name][status] += 1
                self.poll_ms[name].append((time.monotonic() - before) * 1000)
                if status != "200":
                    self.unhealthy.append([round(tick), name, status])
            self.timeline.append([round(tick), self.events["push"], self.events["merge"]])
            await asyncio.sleep(max(0, self.interval - (time.time() - tick)))

    async def stream(self, http, end):
        while time.time() < end:
            url = self.base + "/api/v1/events" + (f"?after={self.last_seq}" if self.last_seq else "")
            try:
                # Connecting and receiving the response headers is bounded (a server that accepts
                # and never answers must not hang the monitor); only the open stream itself is not.
                connect = asyncio.ensure_future(http.get(url, timeout=self.stream_timeout()))
                try:
                    response = await asyncio.wait_for(asyncio.shield(connect), min(self.header_timeout,
                                                                                   max(0.1, end - time.time())))
                except BaseException:
                    # Wait for the cancelled request to finish, so no connection outlives the attempt.
                    connect.cancel()
                    await asyncio.gather(connect, return_exceptions=True)
                    raise
                async with response:
                    self.stream_connects[str(response.status)] += 1
                    if response.status != 200:
                        await asyncio.sleep(2)
                        continue
                    kind, data = None, []
                    while time.time() < end:
                        try:
                            raw = await asyncio.wait_for(response.content.readline(), max(0.1, end - time.time()))
                        except asyncio.TimeoutError:
                            return
                        if not raw:
                            break
                        line = raw.decode(errors="replace").rstrip("\r\n")
                        if line.startswith("event:"):
                            kind = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].strip())
                        elif line == "" and data:
                            resync = self.handle(kind, "\n".join(data))
                            kind, data = None, []
                            if resync:
                                break
            except asyncio.TimeoutError:
                self.stream_connects["header_timeout"] += 1
            except Exception:
                self.stream_connects["error"] += 1
                await asyncio.sleep(min(1, max(0, end - time.time())))

    def handle(self, kind, payload, now=None):
        """Record one event; returns True for a resync (the caller reconnects)."""
        try:
            event = json.loads(payload)
        except ValueError:
            return False
        now = time.time() if now is None else now
        previous = self.last_event_wall if self.last_event_wall is not None else self.observe_start
        if previous is not None:
            self.max_event_gap = max(self.max_event_gap, now - previous)
        self.last_event_wall = now
        kind = event.get("type", kind)
        self.events[kind] += 1
        seq = event.get("seq")
        if kind == "resync":
            self.resyncs.append({"at_wall": round(now, 3), "seq": seq, "pending_pushes_dropped": len(self.unmerged)})
            self.dropped_by_resync += len(self.unmerged)
            self.unmerged.clear()
            # Reconnect from the live edge: sequence numbers may have restarted.
            self.last_seq = None
            return True
        if isinstance(seq, int):
            self.last_seq = max(self.last_seq or 0, seq)
        try:
            at = parse_at(event["at"])
        except Exception:
            return
        if kind == "push":
            self.unmerged[seq] = at
        elif kind == "merge":
            through = event.get("throughSeq", 0)
            for s in [s for s in self.unmerged if s <= through]:
                self.lags.append(at - self.unmerged.pop(s))
        return False

    def gap_until(self, end):
        previous = self.last_event_wall if self.last_event_wall is not None else self.observe_start
        return max(self.max_event_gap, end - previous)

    async def run(self):
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=30, sock_connect=10)
        self.stream_timeout = lambda: aiohttp.ClientTimeout(total=None, sock_connect=10)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=self.trust_env) as http:
            return await self.observe(http)

    async def observe(self, http):
        """Poll and stream until the window ends; then cancel whatever is still running, so no
        stuck request can hold the monitor past its window (plus a short grace)."""
        self.observe_start = time.time()
        end = self.observe_start + self.seconds
        tasks = [asyncio.ensure_future(self.poll(http, end)), asyncio.ensure_future(self.stream(http, end))]
        try:
            _, late = await asyncio.wait(tasks, timeout=self.seconds + self.grace_seconds)
            self.cancelled_at_window_end = len(late)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return self.summary(min(time.time(), end))

    def summary(self, observed_until):
        return {
            "schema": "aspen-place-monitor-v1", "started": round(self.observe_start), "seconds": self.seconds,
            "polls": {k: dict(v) for k, v in self.polls.items()},
            "poll_ms_p50_p99": {k: [pct(v, .5), pct(v, .99)] for k, v in self.poll_ms.items()},
            "unhealthy_polls": self.unhealthy[:200], "unhealthy_poll_count": len(self.unhealthy),
            "events": dict(self.events), "stream_connects": dict(self.stream_connects),
            "max_seconds_between_events": round(self.gap_until(observed_until), 2),
            "resyncs": self.resyncs,
            "merge_lag_seconds": {"complete": not self.resyncs, "pushes_dropped_by_resync": self.dropped_by_resync,
                                  "pushes_merged": len(self.lags), "p50": pct(self.lags, .5),
                                  "p99": pct(self.lags, .99), "max": max(self.lags) if self.lags else None},
            "pushes_unmerged_at_end": len(self.unmerged),
            "tasks_cancelled_at_window_end": self.cancelled_at_window_end,
            "timeline": self.timeline,
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="e.g. https://place.example.org")
    parser.add_argument("--seconds", type=float, default=480)
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--use-env-proxy", action="store_true", help="honour HTTPS_PROXY (sandboxed control hosts)")
    args = parser.parse_args(argv)
    monitor = Monitor(args.base, args.seconds, args.interval, args.use_env_proxy)
    print(json.dumps(asyncio.run(monitor.run()), separators=(",", ":")))


if __name__ == "__main__":
    main()
