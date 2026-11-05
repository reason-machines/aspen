# Benchmark campaign (node D2)

This directory holds Aspen's benchmark campaign: the script that runs it, the
raw results of each published run, and this write-up. The numbers compare
Aspen with stock Git's own server (`git http-backend`, which runs `git
upload-pack` and `git receive-pack`) on the same machine, for the workloads
in section 4.6 of [docs/PLAN.md](../../docs/PLAN.md).

- [run.sh](run.sh) runs the whole campaign and writes a report.
- [results/2026-11-04-cloud-4vcpu/](results/2026-11-04-cloud-4vcpu/) is the
  first published run: one JSON result per workload and target, and
  [REPORT.md](results/2026-11-04-cloud-4vcpu/REPORT.md), the full tables,
  generated from those files by `aspen-bench report`.

Read the limits below before quoting a number. In short: one 4-vCPU
machine ran the server and every client, its memory was resized from 8 GB
to 16 GB partway through (so some git.git comparisons pit Aspen at 8 GB
against stock at 16 GB), Aspen ran as `aspen dev` (SQLite and local disk,
not Postgres and S3), and the stock baseline ran behind a test bridge, not
Apache.

## Results at a glance

Run of 2026-11-04 on one 4-vCPU cloud machine, git 2.39.5 clients. "Stock"
is `git http-backend`. Bytes are the packs the clients stored.

The machine had 8.35 GB of memory for the first part of the run and
16.94 GB for the rest (each result records it in
`environment.memory_bytes`):

| Memory | Groups and targets |
|---|---|
| 8.35 GB | `synthetic` (aspen, aspen-no-compaction, local), `storm1000` (aspen, local), and Aspen's git.git `writers`, warm, cold and depth-1 storms (`real-git/aspen`, including the depth-1 run with 1,039 failures) |
| 16.94 GB | Aspen's git.git `clone`, every stock git.git run (`real-git/local`), both `real-git-threshold` runs (250 and 500 jobs), and the 1,000-push `stale-branch-nightly` cross-check (aspen, aspen-no-compaction, local) |

Byte counts do not depend on memory. Every git.git latency or throughput
comparison except the clone sets Aspen at 8 GB against stock at 16 GB; the
tables mark them.

### Bytes against stock `git upload-pack`

| Fetch | Aspen bytes per fetch | Stock bytes per fetch | Aspen / stock |
|---|---|---|---|
| Depth-1 clone, git.git at v2.47.0 (1,000-job storm) | 12,472,451 | 12,122,961 | 1.029 |
| Depth-1 clone, synthetic repository (1,000-job storm) | 401,767 | 401,780 | 1.000 |
| Full clone, git.git | 252,760,712 | 252,760,712 | 1.000 |
| Cold fetch of the full history, git.git | 252,777,606 | 252,710,963 | 1.0003 |
| One new commit on a warm client, git.git (1,000-job storm) | 16,923 | 17,425 | 0.971 |
| One new commit on a warm client, synthetic (1,000-job storm) | 2,449 | 2,423 | 1.011 |
| Stale-branch fetch, branch created 10,000 trunk pushes ago | 409 | 409 | 1.000 |

The launch gate asks for at most 1.2 times stock's bytes on the depth-1
storm. Aspen's exact shallow packs (node C8) give 1.029 on a real
repository and 1.000 on the synthetic one.

### The stale-branch fetch

A client that has trunk at its tip fetches a branch created N trunk pushes
ago. `visited` is `PlanStats.visited`; "loaded" is how many ref versions the
fetch read from the metadata store (ref versions only: cover and pack-row
reads are not counted).

| N | visited, with compaction | visited, without | loaded, with / without | planning p50, with / without | fetch p50: Aspen with / without / stock |
|---|---|---|---|---|---|
| 10 | 11 | 11 | 33 / 33 | 15.1 / 1.4 ms | 82 / 67 / 33 ms |
| 100 | 20 | 101 | 33 / 225 | 15.3 / 12.4 ms | 85 / 79 / 38 ms |
| 1,000 | 26 | 1,001 | 39 / 2,017 | 16.8 / 111 ms | 89 / 182 / 48 ms |
| 10,000 | 35 | 10,001 | 49 / 10,002 | 17.9 / 563 ms | 126 / 667 / 82 ms |

With compaction the walk grows roughly with log N (four points; 10 cover
chains skipped at N = 10,000); without it, it visits every version. Planning stays under
20 ms at N = 10,000, against 563 ms without compaction.

Two things in this table are not explained yet:

- **The planning floor with compaction.** At N = 10 both servers load the
  same 33 versions, yet planning takes 15.1 ms with compaction against
  1.4 ms without. `loaded` counts ref versions only, not cover or pack-row
  reads, so whatever costs the 14 ms is not in it. The likely cause, from
  the code but not profiled, is loading every cover of the refs the fetch
  touches (about 670 covers of trunk at N = 10,000) and their pack rows.
  The 1,000-push cross-check below fits that: on a trunk a tenth as long,
  N = 10 plans in 2.4 ms with compaction against 1.3 ms without (it ran at
  16.94 GB, so the two runs are not like for like).
- **Loaded against visited.** Without compaction, `loaded` is visited + N
  plus 12 to 24 for every N but the run's largest (33, 225 and 2,017 for
  N = 10, 100 and 1,000: 12, 24 and 16 extra), but only visited + 1 at
  N = 10,000 (10,002). The run's largest N is the branch created on the
  seed itself, at the bottom of trunk, so there is nothing below its branch
  point to read. Why the loader reads about N versions beyond the walk at
  the other points was not traced.

A shorter cross-check, the `nightly` profile's 1,000 trunk pushes
(N = 10, 100 and 1,000; 3 fetches each), is committed as
`stale-branch-nightly` and shown in
[REPORT.md](results/2026-11-04-cloud-4vcpu/REPORT.md). It ran at
16.94 GB, after the main run, from a working tree on `d42c286` (the commit
its `aspen_commit` field records) with uncommitted harness and docs
changes. Its server code is not identical to the main run's: `d42c286`
also carries main's D4, D5 and L0 changes (#115, #112, #136), merged into
this branch after the main run's build, to `aspen-auth`, `aspen-cli` and
`aspen-server` (access and rate limits, SSH limits, the listener and
request-body handling). The planner, metadata, pack, protocol, core and
compaction crates are unchanged. Without
compaction it shows the same shape: 33, 225 and 1,002 loaded for 11, 101
and 1,001 visited, the last again the branch on the seed. With compaction
it visits 11, 28 and 49 versions, but at N = 100 it still loads 225, as
many as without compaction (6 chain skips). Why is untraced too.

### Load

| Workload | Aspen | Stock |
|---|---|---|
| 1,000 warm jobs fetch one new commit, synthetic (3 rounds) | p50 4.4 s, 195 fetches/s | p50 27.3 s, 31 fetches/s |
| 1,000 depth-1 jobs, synthetic (3 rounds) | p50 5.1 s, 137 fetches/s | p50 29.7 s, 28 fetches/s |
| 1,000 warm jobs fetch one new commit, git.git (2 rounds; Aspen 8 GB, stock 16 GB) | p50 13.4 s, 63 fetches/s | p50 39.6 s, 19 fetches/s |
| 1,000 depth-1 jobs, git.git (2 rounds; Aspen 8 GB, stock 16 GB) | completed 961 of 2,000 (**1,039 failed**); p50 51.6 s over those 961 | completed all 2,000; p50 472 s |
| 16 writers for 30 s, synthetic: push→fetch | p50 202 ms, 64 pushes/s | p50 758 ms, 37 pushes/s |
| 16 writers for 30 s, git.git: push→fetch (Aspen 8 GB, stock 16 GB) | p50 198 ms, 12.4 pushes/s | p50 1,563 ms, 14.2 pushes/s |
| 4 clients clone git.git (8 clones; both 16 GB) | p50 29.4 s | p50 32.9 s |

The two depth-1 latencies on git.git are not comparable: Aspen's p50 covers
only the fetches that succeeded, and the 1,039 that failed stopped
competing for CPU when they hit the 30 s pool timeout, while stock's 2,000
fetches all ran to the end.

## Where Aspen loses

Each of these is in the raw results; the two product problems have issues.

1. **Push latency grows with trunk history**
   (#149). Over
   10,000 single-commit trunk pushes, Aspen's push p50 went from 28.6 ms
   (first 1,000) to 185 ms (last 1,000); stock stayed at 30–31 ms. Turning
   compaction off changes nothing (180 ms). Every push walks the trunk
   version's whole dependency closure to choose the packs it searches for
   thin bases (`receive::closure::closure_packs`), and that step is in none
   of the push log's timed stages.
2. **Fetch storms exhaust the metadata pool**
   (#150). With 1,000
   concurrent depth-1 fetches of git.git, about half failed with "internal
   storage error; retry": the server log shows `pool timed out while
   waiting for an open connection` for each one. 250 and 500 concurrent
   jobs had no failures
   ([real-git-threshold](results/2026-11-04-cloud-4vcpu/REPORT.md)), but
   those two runs had the 16 GB machine (the 1,000-job run had 8 GB) and
   ran against a separately started `aspen dev` on port 8091 with
   `aspen-bench` called directly, not through `run.sh` (command under
   [Reproduce](#reproduce)). The pool had 10 connections and a 30 s
   acquire deadline, and no setting changed them. Fixed by the #150
   admission queue (docs/PLAN.md decisions log): afterwards all 1,000
   succeeded. Stock completed all 2,000 fetches (p50 472 s); Aspen
   completed 961 (p50 51.6 s), so the latencies are not comparable.
3. **Single fetches of a stale branch are slower than stock.** At N = 10
   Aspen took 82 ms against stock's 33 ms; at N = 10,000, 126 ms against
   82 ms. Part of it is a planning floor that compaction adds: planning at
   N = 10 takes 15.1 ms with compaction against 1.4 ms without, though both
   read the same 33 versions. The likely cause, from the code but not
   profiled, is that each fetch loads every cover of the refs it touches
   (about 670 covers of trunk at N = 10,000) and their pack rows. The floor
   stays flat as N grows.
4. **Push tail latency and push rate on a real repository.** 16 writers on
   git.git: push p99 2,983 ms for Aspen against 1,356 ms for stock (p50
   1,031 against 1,056 ms), and 12.4 pushes/s against 14.2. Aspen ran with
   8 GB of memory here and stock with 16 GB. Not investigated further.
5. **Bytes.** The depth-1 pack of git.git is 2.9% larger than stock's (the
   exact shallow pack stores whole any delta whose base is outside the
   region; decisions log, C8). A cold full fetch is 0.03% larger.

## Method

### Hardware and software

| | |
|---|---|
| Machine | One cloud virtual machine, 4 vCPUs (`Intel(R) Xeon(R) Processor`), Linux 6.1, overlay filesystem. Memory 8.35 GB, then 16.94 GB: the machine was resized between Aspen's git.git depth-1 storm and its git.git clone (which groups ran at which size is under [Results at a glance](#results-at-a-glance)) |
| Placement | Server and every client on the same machine, over loopback |
| Git (clients and stock server) | 2.39.5 (Debian 12) |
| Aspen | `aspen dev` built in release mode from this repository: SQLite metadata, local-disk blob store, compaction worker on (except `aspen-no-compaction`), exact shallow packs on. Built from main at `6578ec9` plus the first commit of the PR that adds this directory (#148, "feat(bench): stale-branch workload, planner load metrics, campaign profiles"), which adds the planner metrics and `--no-background-worker`. The results' `aspen_commit` field is null for this run: the runs were started before `run.sh` set `ASPEN_BENCH_COMMIT`, from a working tree that was committed afterwards, so no commit ID existed to record. `run.sh` fills it in from now on |
| Stock | `git http-backend` behind `aspen-testkit`'s bridge (`--url local`): one thread per connection, CGI per request, response buffered before it is sent |
| Each result's `environment` field | git version, CPU model, CPU count, memory, kernel |

Every simulated client is a real `git` process; latencies are the wall time
of that process. Received bytes are the sizes of the packs each client
stored (`transfer.unpackLimit=1`). Client housekeeping is off. See
[bench/README.md](../README.md) for exactly what each workload does.

### Repositories

- **Synthetic** (`--repo-size small`: 200 files of 4 KiB, 20 commits;
  `medium` for clones and the stale-branch run: 5,000 files of 8 KiB, 200
  commits), generated deterministically by `aspen-bench`.
- **git.git at v2.47.0** (`777489f9e09c`, 75,031 commits, 252 MB packed),
  fetched from GitHub once and pushed to each target as one push
  (`--repo-source`).

### What each group ran

| Group | Profile (`bench/baselines/run.sh`) | Targets | Memory |
|---|---|---|---|
| `synthetic` | `full` for `stale-branch` (N = 10, 100, 1,000, 10,000; 5 fetches each); `nightly` for the rest | aspen, aspen-no-compaction (stale-branch only), local | 8.35 GB |
| `storm1000` | `storm1000`: warm and depth-1 storms of 1,000 jobs × 3 rounds | aspen, local | 8.35 GB |
| `real-git` | `real` with git.git: 16 writers for 30 s, storms of 1,000 jobs × 2 rounds (warm and depth-1) and 8 × 2 (cold), 4 clients × 8 clones | aspen, local | aspen: 8.35 GB, except `clone` at 16.94 GB; local: 16.94 GB |
| `real-git-threshold` | the depth-1 storm on git.git at 250 and 500 jobs, one round each, against one separately started `aspen dev` on port 8091 (not through `run.sh`; see [Reproduce](#reproduce)) | aspen | 16.94 GB |
| `stale-branch-nightly` | `nightly` for `stale-branch` (1,000 trunk pushes; N = 10, 100, 1,000; 3 fetches each), through `bench/baselines/run.sh` directly, as a cross-check after the main run | aspen, aspen-no-compaction, local | 16.94 GB |

### The >2 GiB `.idx` check

The decisions log deferred to D2 a comparison of Aspen's `.idx` with `git
index-pack`'s for packs past 2 GiB, where `.idx` v2 uses 64-bit offsets.
`a_pack_over_2_gib_indexes_byte_identical_to_git` (ignored in required CI,
run by the nightly `bench` job and by `run.sh idx`) builds a 2.35 GB pack
(280 random 8 MiB blobs and 64 deltified text blobs after them), indexes it
with both, and requires identical bytes. On this machine: identical, 88
offsets in the 64-bit table; `git index-pack` took 13.4 s and Aspen's
receive and complete 11.2 s. These figures are from the test's console
output; the test writes no result file.

## Limits of this run

- **One machine.** The plan asks for identical instances with the client on
  separate hardware. Here the clients compete with the server for 4 vCPUs,
  which hurts whichever server needs more CPU per request, and storms of
  1,000 `git` processes are mostly client CPU. Latency and throughput
  comparisons are indicative only; byte counts are not affected.
- **`aspen dev`, not production.** Metadata is SQLite and packs are on
  local disk. `aspen serve` with Postgres and S3 (and express or a CDN)
  was not measured. Clients did not set `fetch.uriprotocols`, so every
  fetch got a stitched pack.
- **The stock baseline is the bridge,** not Apache: the Docker baselines
  (Apache `git http-backend`, Gitea, Forgejo, GitLab) need Docker, which
  this machine does not have. The nightly job runs them on GitHub's
  runners.
- **One client version,** git 2.39.5, over HTTP with protocol v2. SSH and
  other clients were not measured.
- **The `full` profile's 1,000 writers** were not run; 16 writers were.
- **Memory changed mid-campaign** (8.35 GB, then 16.94 GB; see
  [Results at a glance](#results-at-a-glance)). Bytes are unaffected; the
  git.git latency comparisons marked "Aspen 8 GB, stock 16 GB" are not
  like for like.
- Each number is one run, with no repeats, except the git.git depth-1 storm,
  which ran twice (1,039 and 940 failures of 2,000). Every measured result
  in this write-up comes from the committed JSON, with two exceptions, both
  taken from console output: that second run's 940 (its result file was
  not kept, and its memory size was not recorded) and the
  [>2 GiB `.idx` check](#the-2-gib-idx-check)'s pack size, offset count
  and timings (the test writes no result file). Configuration values such
  as the pool size, the 30 s timeout and the push counts come from the code
  and the profiles, not from results. Treat latency differences of a few
  percent as noise.

## Reproduce

From a clean checkout, with what [bench/README.md](../README.md) lists:

```bash
bench/campaign/run.sh /tmp/aspen-campaign            # every step, about 2 hours
bench/campaign/run.sh /tmp/aspen-campaign stale real # some steps
target/release/aspen-bench report /tmp/aspen-campaign
```

The `threshold` step runs the 250- and 500-job depth-1 storms through
`run.sh`, with a fresh server per size. The published threshold results
were not made that way: both ran against one `aspen dev` started by hand,
with `aspen-bench` called directly. Reconstructed from the parameters the
result files record, the commands were:

```bash
target/release/aspen dev --listen 127.0.0.1:8091 --data-dir /tmp/aspen-t &
for jobs in 250 500; do
  DATABASE_URL=sqlite:///tmp/aspen-t/meta.db target/release/aspen repo create "t$jobs"
  target/release/aspen-bench fetch-storm --url "http://127.0.0.1:8091/t$jobs.git" \
    --repo-source /tmp/aspen-campaign/.source/git.git --branch-scoped-push \
    --mode cold --depth 1 --jobs "$jobs" --rounds 1 \
    --out "/tmp/aspen-campaign/real-git-threshold/aspen-$jobs-jobs/fetch-storm-depth1.json"
done
```

The `stale-branch-nightly` cross-check is not a `run.sh` step. It ran
through the baselines script directly, once per target, with commands
equivalent to:

```bash
for t in aspen aspen-no-compaction local; do
  BENCH_PROFILE=nightly BENCH_WORKLOADS=stale-branch \
    BENCH_OUT=/tmp/aspen-campaign/stale-branch-nightly/$t bench/baselines/run.sh $t
done
```

To publish a run, copy the result directory here as
`results/<date>-<machine>/` with its `REPORT.md`, and update the tables
above from it.
