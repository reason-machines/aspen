# Bounded Place API throughput runner

This runner preserves the externally paced live-load method as a reusable tool.
It does not provision machines, change the target, or send traffic by default.
Use Python 3.10 or newer. The mock tests and dry run use only the standard library;
execution also needs `aiohttp` from `requirements.txt`. Request and deadline
timeouts handle both built-in `TimeoutError` and `asyncio.TimeoutError`, which
are distinct exception classes on Python 3.10.

## What it measures

Each offered request paints one tile through `POST /api/v1/pushes`. The Place
server turns that into **one in-process Git receive-pack push**, with a pack,
index, entry table and metadata commit durable before its `201` response. This
is the real Place API path, not stock Git's network transport. It does not measure
Git client pack construction, smart-HTTP advertisement/negotiation, SSH, a fetch,
merge-to-main latency, or full push-to-fetch latency. Use the existing
[`aspen-bench` workloads](../README.md) for stock-Git traffic.

A success needs `201`, a canonical commit ID and a positive integer sequence.
A repeated commit ID within a worker is a failure, not another successful push.
There are **no retries**, including session creation. Timeouts, disconnects and
deadline cancellations have unknown server outcomes, so they never count as
successes. Acknowledgements are not an independent durability audit; follow up
with a stock-Git fetch/read-back and server metrics when reporting a live result.

## Default workload and limits

Run one process on each of **12 independently addressed runners**, indexed 0..11.
Each creates 110 sessions sequentially before a shared start, then schedules:

- A 60-second ramp, five 12-second steps at 10, 20, 30, 40, then 50 offers/s
  per worker (120, 240, 360, 480, then 600 in total).
- A 300-second hold at 50 offers/s per worker, or **600 offered requests/s**.
- At most 128 requests in flight per worker, and one per session.
- A 30-second request timeout and an absolute 420-second worker deadline,
  covering setup, start wait, scheduling and drain. An impossible common start
  fails before creating any session. A past or missed start never starts a late run.

The default schedule has 1,800 ramp offers and 15,000 hold offers per worker:
201,600 total offers across 12 workers. Offers are timed from the common start,
not from the previous response. Worker offsets spread requests within each tick.
A busy session, full in-flight limit, or schedule lag above one second skips the
offer instead of queueing it. The scheduler never waits for a request to finish
to issue another offer. Record client skips; do not call 600 scheduled offers/s
600 sent or accepted pushes/s.

The current Place defaults are **30 pushes/minute/session**, **120 new
sessions/minute/client address**, and a **550 pushes/s server-wide ceiling** with
a 2,750-push burst. At 50/s over 110 sessions, each session sees at most 28
offers in a rolling minute, below 30. Creating 110 sessions leaves only 10 of
one address's session-creation allowance, so use clean addresses with no recent
session setup. Twelve processes behind one NAT still share one address and
will fail session creation. Never spoof forwarding headers to avoid the limit.
The server's IPv6 address key is a /64, not an individual IPv6 address.

At 600 offered/s the ceiling should eventually refuse requests with `429`;
550 is a ceiling, not a guaranteed achieved rate. The burst and existing traffic
affect when refusals start. Existing visitors consume the same ceiling, while
internal painters/merges have their own policies. Do not raise server limits to
make the benchmark pass. Other API refusals and client-side loss stay visible.

**Live load is postponed; the full 600 offers/s test has not been executed.**
Personas must run uninterrupted for 20 hours and then remain running permanently.
A sustained hold above approximately 500 offers/s risks persona backoff: once
all external traffic exceeds the actual 550/s ceiling, it can drain the bucket
below 10% and pause the personas. The full
test therefore cannot run while personas are protected. The execution commands
below are reference instructions, not authorization to run them now. Even an
optional subceiling probe would require live bucket monitoring to keep usage
below 50%; that monitor is not currently exposed by the API, so all live load,
including probes, is skipped.

The runner refuses `--execute` when `--rate` × `--workers` exceeds 500 unless
`--acknowledge-persona-impact` is also explicitly supplied. Dry runs remain
available at any configured rate. This acknowledgement is a human choice for
a separately authorized future window, not permission to override the current
prohibition or proof that a rate at or below 500 is safe for existing traffic.
Neither execution nor acknowledgement can be enabled through the environment.

The payload uses palette colors in the top-left 16x16 tile area, matching the
unexecuted scratch generator. **This changes real canvas content and creates branches and
objects.** Run only on an explicitly authorized target whose rules allow this
area; this tool does not discover keep areas or restore previous paint. Do not
run against Reason production or any unrelated service.

## Install and inspect without traffic

From the repository root, on each runner:

```bash
python3 -m venv bench/throughput/.venv
bench/throughput/.venv/bin/python -m pip install -r bench/throughput/requirements.txt
python3 -m unittest discover -s bench/throughput -v
python3 bench/throughput/place_load.py
python3 bench/throughput/place_load.py --target-url https://place.example.org/api/v1
```

Both last commands only print the plan. Even setting `ASPEN_LOAD_TARGET_URL`
never opts into execution. Only `--execute` does that, and it also requires an
explicit target URL. Remote targets must use HTTPS. Plain HTTP is allowed only
on loopback for an authorized local test. Targets cannot carry user info, query
parameters or fragments. Redirects are refused rather than followed with a token.
Session tokens remain in process memory; output contains no response bodies,
headers, tokens, target URL, or arbitrary exception/server-message text.

## Execute on an existing, non-AWS-specific runner pool

Prepare 12 machines with Python, the three Python files and requirements under
`~/aspen/bench/throughput/`. Install the dependencies using the commands above.
Give them distinct egress addresses, working TLS trust, synchronized clocks and
network access to the **authorized** target. No cloud credentials are needed by
the generator. The following example uses existing SSH hosts named
`load-0` through `load-11`; it creates no machines or cloud resources.

Only after the live window and target are authorized, run this from the control
machine. The 600/s example deliberately omits `--acknowledge-persona-impact`
and will refuse execution; only an explicitly authorized human decision may add
that flag once the current persona-protection prohibition no longer applies.
All workers must be launched promptly; the 25-second lead leaves
roughly five seconds of slack inside the 420-second deadline in addition to the
full request drain. If setup misses it, abort the campaign rather than restarting
all workers immediately on rate-limited addresses.

```bash
mkdir -p load-results
START_AT=$(python3 -c 'import time; print(time.time() + 25)')
RUN="place-$(date -u +%Y%m%dT%H%M%SZ)"
pids=()
for i in $(seq 0 11); do
  ssh "load-$i" "cd ~/aspen && bench/throughput/.venv/bin/python bench/throughput/place_load.py \
    --execute --target-url https://place.example.org/api/v1 \
    --worker $i --workers 12 --rate 50 --sessions 110 \
    --ramp-seconds 60 --hold-seconds 300 --deadline-seconds 420 \
    --start-at $START_AT --run $RUN" >"load-results/worker-$i.jsonl" 2>"load-results/worker-$i.stderr" &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
printf 'worker command failure: %s\n' "$failed"
python3 bench/throughput/aggregate.py load-results/worker-*.jsonl >load-results/aggregate.json
```

These are Bash commands. Replace the example hostname in the command with the
actual authorized API base. If a shell/service imposes a wall-time limit, allow
at least 420 seconds and collect each worker's stdout independently. Stop the
workers with Ctrl-C or the runner platform's stop control if necessary; an
interrupted process may have no result and the aggregator will flag it missing.
No process should continue offering after 360 seconds from the shared start.
The script stops after its drain/deadline without needing an operator timer.

For one standalone authorized runner, omit `--start-at` and it picks a start
using half the spare deadline budget (15 seconds with the defaults). This is
not a synchronization mechanism for a multi-worker campaign. CLI flags override
environment variables of the same name, prefixed with `ASPEN_LOAD_` and written
in uppercase with underscores. Examples are `ASPEN_LOAD_WORKER`,
`ASPEN_LOAD_START_AT`, `ASPEN_LOAD_TARGET_URL`, and `ASPEN_LOAD_RUN`.
`--execute` and `--acknowledge-persona-impact` deliberately have no
environment-variable equivalents.
Use `python3 bench/throughput/place_load.py --help` for all options.

## Results and aggregation

Each worker prints one JSON object containing `result`. It exits 2 for fatal
setup/deadline errors and 0 after a fully scheduled campaign, even when pushes
were refused: a measured `429` is data, not a crashed generator.
For both `ramp` and `hold`, it reports:

- `scheduled`, `sent`, `skipped` by client reason, `success`, and `failures` by
  bounded reason. HTTP 429 distinguishes the global ceiling, session quota and
  other rate limits. Transport errors preserve their unknown-outcome status.
- Successful-request latency as counts in integer-millisecond ceiling buckets.
  Latency begins when a sent task starts its request, excluding scheduler lag;
  `max_schedule_lag_ms` is reported separately.

The equations are `scheduled = sent + skipped` and
`sent = success + failures`. A deadline result is partial even if scheduling
finished, because draining did not finish normally. Session setup is not mixed
into push throughput or push latency; setup failure is a worker fatal error.

```bash
python3 bench/throughput/aggregate.py load-results/worker-*.jsonl
python3 bench/throughput/aggregate.py cloud-log-export.json --expected-workers 12
```

The aggregator accepts JSONL, raw result objects and cloud log exports with
`events`, `logEvents`, `message` or `@message` fields. It also accepts the earlier
scratch generator's `counts`/`latency_histogram_ms` result shape. Legacy client
skips and transport failures cannot be split into the newer specific reasons.
A legacy result with duplicate commits is rejected because its success latency
buckets cannot be corrected. Raw legacy error messages are never echoed.

Repeated identical worker results are deduplicated. Conflicting copies,
incompatible starts/configurations, or counters that do not reconcile fail
closed. Missing/fatal/unfinished workers make `complete` false and return exit
1; malformed or inconsistent evidence returns exit 2. Exit 0 means all worker
results are present and finished, **not** that a throughput target was met.
Rates divide phase counts by the configured phase length, not by the sum of
worker elapsed times. Histograms are merged by adding bucket counts before
computing p50/p95/p99; worker percentiles are never averaged. Failure latency is
not in this histogram, so always report failures and skips beside it. Duplicate
commit detection is within a worker only; aggregation cannot establish global
uniqueness from the compact result objects.

## Cost and cleanup

Budget approximately **US$3–5 for this short live window**, not a price promise
or a spending authorization. At most 201,600 offered pushes would write about
604,800 S3 objects (pack, index, entry table), approximately US$3.02 in PUT
requests alone at an illustrative US$0.005/1,000 PUTs. Rate-limit refusals write
nothing, but existing traffic, background writes, reads, logs, storage, database,
network transfer, client machines and server runtime all add cost. Region,
provider, pricing, startup time and resources left running change the bill.
An AWS budget alarm notifies; it is not an automatic spend cutoff.

Use existing authorized capacity where possible, keep the worker deadline,
record actual usage, then stop temporary runners and remove only the temporary
resources you created through their normal teardown. The runner does not delete
server branches or objects: let the target's branch TTL and normal garbage
collection policy apply. Do not reset the canvas/history or change production
resources as benchmark cleanup.
