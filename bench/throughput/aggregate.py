#!/usr/bin/env python3
"""Merge worker result objects, including JSONL and exported cloud-log messages."""

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sys

from place_load import PHASES, refusal_reason


def results_in(value):
    if isinstance(value, str):
        try:
            yield from results_in(json.loads(value))
        except ValueError:
            return
    elif isinstance(value, list):
        for item in value:
            yield from results_in(item)
    elif isinstance(value, dict):
        if "worker" in value and ("phases" in value or "latency_histogram_ms" in value):
            yield value
        else:
            for key in ("result", "message", "events", "logEvents", "results", "@message"):
                if key in value:
                    yield from results_in(value[key])


def load_results(paths):
    for path in paths:
        text = Path(path).read_text()
        try:
            value = json.loads(text)
        except ValueError:
            for line in text.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                yield from results_in(value)
        else:
            yield from results_in(value)


def legacy_phases(result):
    counts = result["counts"]
    phases = {}
    for phase in PHASES:
        prefix = phase + "_"
        duplicates = counts.get(prefix + "duplicate_commit", 0)
        if duplicates:
            raise ValueError("legacy result contains duplicate commits; its latency buckets cannot be corrected")
        failures = Counter()
        statuses = Counter()
        for key, count in counts.items():
            suffix = key.removeprefix(prefix)
            if key.startswith(prefix) and suffix.startswith("status_"):
                status = suffix.removeprefix("status_")
                statuses[status] += count
            elif key.startswith(prefix) and suffix.startswith("exception_"):
                failures["legacy_transport_error_outcome_unknown"] += count
        for raw_reason, count in result.get("reasons", {}).items():
            parts = raw_reason.split(":", 3)
            if len(parts) == 4 and parts[0] == phase and parts[1] in statuses:
                if count > statuses[parts[1]]:
                    raise ValueError("legacy reason count exceeds status count")
                statuses[parts[1]] -= count
                failures[refusal_reason(int(parts[1]), {"message": parts[3]})] += count
        for status, count in statuses.items():
            if count:
                failures["http_" + status if status.isdecimal() else "legacy_http_failure"] += count
        phases[phase] = {"scheduled": counts.get(prefix + "scheduled", 0),
                         "sent": counts.get(prefix + "sent", 0),
                         "success": counts.get(prefix + "success", 0),
                         "skipped": {"legacy_client": counts.get(prefix + "skipped_client", 0)},
                         "failures": dict(failures),
                         "latency_histogram_ms": result["latency_histogram_ms"].get(phase, {})}
    return phases


def percentiles(histogram):
    total = sum(histogram.values())
    if not total:
        return {key: None for key in ("p50", "p95", "p99")}
    values = {}
    for key, fraction in (("p50", .5), ("p95", .95), ("p99", .99)):
        remaining = math.ceil(total * fraction)
        for bucket, count in sorted(histogram.items()):
            remaining -= count
            if remaining <= 0:
                values[key] = bucket
                break
    return values


def merge(results, expected_workers=12):
    by_worker = {}
    for result in results:
        worker = result["worker"]
        if type(worker) is not int or not 0 <= worker < expected_workers:
            raise ValueError("worker outside expected range")
        if worker in by_worker and by_worker[worker] != result:
            raise ValueError("conflicting results for one worker")
        by_worker[worker] = result
    if not by_worker:
        raise ValueError("no worker results found")
    records = list(by_worker.values())
    if len({row["run"] for row in records}) != 1 or len({row["start"] for row in records}) != 1:
        raise ValueError("workers must share a run ID and common start")
    signatures = set()
    for row in records:
        config = row.get("config", {})
        signatures.add((config.get("ramp_seconds", row.get("ramp_seconds")),
                        config.get("hold_seconds", row.get("hold_seconds")),
                        config.get("rate", 50), config.get("workers", expected_workers)))
    if len(signatures) != 1:
        raise ValueError("workers have different phase durations, rates or worker counts")
    ramp, hold, rate, workers = signatures.pop()
    if workers != expected_workers or hold is None or ramp is None:
        raise ValueError("configured worker count or phase durations missing/mismatched")
    merged = {}
    fatals = sorted(row["worker"] for row in records if row.get("fatal"))
    unfinished = sorted(row["worker"] for row in records if row.get("scheduling_finished") is False)
    for phase, seconds in (("ramp", ramp), ("hold", hold)):
        counts, skipped, failures, histogram = Counter(), Counter(), Counter(), Counter()
        for row in records:
            source = row["phases"] if "phases" in row else legacy_phases(row)
            stats = source[phase]
            values = [stats[key] for key in ("scheduled", "sent", "success")]
            for key in ("skipped", "failures", "latency_histogram_ms"):
                values.extend(stats[key].values())
            if any(type(value) is not int or value < 0 for value in values):
                raise ValueError("counters must be nonnegative integers")
            if any(int(bucket) <= 0 for bucket in stats["latency_histogram_ms"]):
                raise ValueError("latency buckets must be positive milliseconds")
            if (stats["scheduled"] != stats["sent"] + sum(stats["skipped"].values())
                    or stats["sent"] != stats["success"] + sum(stats["failures"].values())
                    or stats["success"] != sum(stats["latency_histogram_ms"].values())):
                raise ValueError("worker counters/histograms do not reconcile")
            counts.update({key: stats[key] for key in ("scheduled", "sent", "success")})
            skipped.update(stats["skipped"])
            failures.update(stats["failures"])
            histogram.update({int(bucket): count for bucket, count in stats["latency_histogram_ms"].items()})
        merged[phase] = {**counts, "skipped": dict(skipped), "failures": dict(failures),
                         "scheduled_per_second": counts["scheduled"] / seconds if seconds else None,
                         "sent_per_second": counts["sent"] / seconds if seconds else None,
                         "acknowledged_per_second": counts["success"] / seconds if seconds else None,
                         "latency_histogram_ms": dict(sorted(histogram.items())),
                         "success_latency_ms": percentiles(histogram)}
    all_hist = Counter()
    for phase in merged.values():
        all_hist.update(phase["latency_histogram_ms"])
    missing = sorted(set(range(expected_workers)) - by_worker.keys())
    return {"schema": "aspen-place-load-aggregate-v1", "run": records[0]["run"],
            "start": records[0]["start"], "workers": sorted(by_worker), "missing_workers": missing,
            "failed_workers": fatals, "unfinished_workers": unfinished,
            "complete": not (missing or fatals or unfinished),
            "hold_target_offers_per_second": rate * expected_workers, "phases": merged,
            "success_latency_ms": percentiles(all_hist),
            "max_schedule_lag_ms": max(row.get("max_schedule_lag_ms", row.get("counts", {}).get("max_schedule_lag_ms", 0)) for row in records),
            "limits": ["201 acknowledgements, not independently fetched commits",
                       "uniqueness checked within each worker, not across workers",
                       "latency covers successful requests only; transport failures have unknown outcomes"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+")
    parser.add_argument("--expected-workers", type=int, default=12)
    args = parser.parse_args(argv)
    try:
        result = merge(load_results(args.logs), args.expected_workers)
    except (ValueError, KeyError, TypeError, OSError):
        print("cannot aggregate: missing, conflicting, incompatible or inconsistent worker evidence", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
