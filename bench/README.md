# aspen-bench

`aspen-bench` is a load generator for Git servers that speak smart HTTP. It
drives stock `git` clients against any repository URL (Aspen, `git
http-backend`, Gitea, Forgejo, GitLab, or anything else) and writes one JSON
result per run. `bench/baselines/` brings up the comparison servers in Docker
and runs every workload against them.

The workloads come from section 4.6 of [docs/PLAN.md](../docs/PLAN.md):

| Workload | What it does | Headline metrics |
|---|---|---|
| `writers` | OpenAI's simulated workload: N writers each push new commits to their own branch; each acknowledged push gets an independent reader that fetches the branch until it sees the new tip | `push`, `push_to_fetch` latency, pushes/s |
| `fetch-storm` | M jobs fetch the same just-pushed commit at the same moment, for several rounds. `--mode warm` jobs already have `main` (cached CI runners); `--mode cold` jobs start empty; `--depth 1` makes them shallow (`--mode cold --depth 1` is the `actions/checkout` default, the plan's depth-1 fetch storm) | `fetch` latency, fetches/s, MiB/s, bytes per fetch |
| `clone` | C concurrent clients repeatedly run `git clone --bare` | `clone` latency, clones/s, MiB/s |
| `stale-branch` | The stale-branch fetch: `max(N)` single-commit trunk pushes, with a branch created N pushes before the end for each N; then a client current on trunk fetches each branch | per N: `stale_fetch_n<N>` latency, bytes received; against Aspen (`--metrics-url`) also `PlanStats.visited`, versions loaded, chain skips and planning time; trunk push latency, first and last tenth |
| `queue` | N clients enqueue changes with Aspen's merge queue (`ASPEN_MERGE_QUEUE=on`) and wait for each outcome, read by one poller with a `ref-prefix` ls-refs request (needs `curl`). `--overlap 70,25,5` mixes changes to a client's own new file, a new file in a shared directory and a rewrite of a shared file (only these can conflict). `--verify-landed` re-merges every landed change onto the parent it landed on with `git merge-tree --write-tree`, compares trees and runs `git fsck --strict`; any mismatch fails the run | `visible` (land request to visible on `main`), `landed`, merged/s, reject rate |

Published results, the method behind them and the script that reproduces
them are in [campaign/](campaign/README.md) (node D2).

## Reproduce from scratch

You need:

- Linux or macOS with `git` 2.31 or newer on `PATH`;
- a Rust toolchain via [rustup](https://rustup.rs) (the repository pins the
  version; rustup installs it on first use);
- for the baselines: Docker with Compose v2 (`docker compose version`) and
  `curl`. GitLab also needs about 4 GB of free memory.

Then, from the repository root:

```bash
# 1. Build and try it against an in-process git http-backend (no Docker).
cargo build --release -p aspen-bench
target/release/aspen-bench writers --url local --writers 8 --pushes-per-writer 10

# 2. Run every workload against one Docker baseline, then tear it down.
bench/baselines/run.sh git-http-backend          # or gitea, forgejo, gitlab

# 3. The same with the larger nightly profile.
BENCH_PROFILE=nightly bench/baselines/run.sh gitea
```

Results land in `bench/results/<target>/<workload>.json` (ignored by Git).
Each run also prints a short summary to stderr:

```text
writers against local-git-http-backend (http://127.0.0.1:42329/bench.git): 1.60s, 0 errors
  push             n=80     p50=131.9ms p90=162.4ms p99=181.3ms max=181.3ms
  push_to_fetch    n=80     p50=194.8ms p90=237.9ms p99=265.2ms max=265.2ms
  reader_fetch     n=80     p50=183.8ms p90=226.0ms p99=242.9ms max=242.9ms
  reader_queue     n=80     p50=6.8ms p90=30.4ms p99=54.5ms max=54.5ms
  pushes_per_second 49.99
```

`run.sh <target> all` is `up`, `run`, then `down`. Use the steps separately
to keep a server running between runs:

```bash
bench/baselines/run.sh gitea up
bench/baselines/run.sh gitea run     # as often as you like
bench/baselines/run.sh gitea down    # deletes the server's data
```

## Running the tool directly

Every workload takes a repository URL. Credentials may be in the URL; the
tool removes them before printing or storing the URL and passes them to `git`
as an HTTP header through environment variables, so they never appear in
process arguments. Use throwaway credentials anyway.

```bash
aspen-bench writers --url http://user:pass@host/org/repo.git \
  --writers 100 --duration-secs 60 --out writers.json
aspen-bench fetch-storm --url ... --jobs 1000 --rounds 3 --mode warm
aspen-bench clone --url ... --concurrency 8 --clones 32 --repo-size medium
aspen-bench <workload> --help        # every option
```

By default a workload first pushes the synthetic history to `main`. The
target repository must be empty, or already hold the same history (same
`--repo-*` options). Pass `--no-seed` if `main` is already there. Benchmark
branches are named `bench/<run-id>/...` and deleted afterwards
(`--keep-branches` keeps them).

`--url local` starts an in-process server that hands each request to stock
`git http-backend` (the bridge from `tests/interop`). It is for development
and the CI smoke; the Docker `git-http-backend` target is the real baseline.

### The stale-branch fetch

```bash
aspen-bench stale-branch --url http://127.0.0.1:8086/repo.git --branch-scoped-push \
  --points 10,100,1000,10000 --repeats 5 --metrics-url http://127.0.0.1:8086/-/metrics
```

All commits are written up front with `git fast-import`, so the pushes
measure the server. Trunk commits are dated after the seed history (Git
sends haves newest first), and each fetch client is a fresh shared clone
that holds only `main` and offers no alternate-ref haves
(`core.alternateRefsCommand=true`), as a real client current on trunk would.
With `--metrics-url` (Aspen only) the workload reads `/-/metrics` around each
fetch: `visited.n<N>` and `chain_skips.n<N>` are `PlanStats.visited` and
`PlanStats.chain_skips`, `loaded_versions.n<N>` is
`aspen_fetch_plan_loaded_versions` (one metadata read each), and
`plan_n<N>` the planning time (`loaded_versions` counts ref versions only,
not cover or pack-row reads). It also waits for the server's background
jobs to settle first: no job queued or running for 30 s, or the run fails
at `--settle-timeout-secs`, so a compacting server is measured after
compaction has caught up and a stalled compactor is an error, not a
result. The `aspen-no-compaction` target runs `aspen dev
--no-background-worker` and passes `--no-background-worker` to the
workload, which then accepts jobs that stay queued, for the same numbers
without covers.

### Real repositories

`--repo-source <bare repository>` (or `BENCH_REPO_SOURCE` for `run.sh`)
uses that repository's `main` as the seed history instead of a synthetic
one; results record its tip as `repo_source_tip`. The campaign uses git.git
at v2.47.0 ([campaign/run.sh](campaign/run.sh) fetches it).

### Reports

`aspen-bench report <dir>` prints Markdown tables for a directory of
`<group>/<target>/<workload>.json` results, with bytes as a ratio to
`--reference` (default `local`). Every row shows the memory of the machine
its result came from (`environment.memory_bytes`, `-` when it wasn't
recorded), since one campaign can mix machine sizes. The campaign's
published `REPORT.md` files are its output.

### Synthetic repositories

`--repo-size tiny|small|medium|large` picks a preset, and `--repo-files`,
`--repo-file-bytes`, `--repo-commits` and `--repo-changes-per-commit`
override it. The same options and `--repo-seed` always produce the same
objects and the same `main` commit, on any machine and Git version: content
comes from a seeded SplitMix64 generator, commits have fixed authors and
dates, and history is written with `git fast-import`. Later commits rewrite a
few lines of a few files, so packs contain realistic deltas.

| Preset | Files | File size | Commits | Files per commit | Raw size |
|---|---|---|---|---|---|
| tiny | 20 | 1 KiB | 3 | 2 | 20 KB |
| small | 200 | 4 KiB | 20 | 10 | 0.8 MB |
| medium | 5,000 | 8 KiB | 200 | 50 | 40 MB |
| large | 50,000 | 8 KiB | 1,000 | 100 | 400 MB |

`aspen-bench generate --repo-size small --out /tmp/small.git` writes one
locally and prints its `main` commit.

### What exactly is measured

- Every latency is the wall time of one `git` process, from spawn to exit,
  as an agent or CI job would see it.
- `push_to_fetch` runs from the moment the writer's `git push` exits
  successfully until a reader's `git fetch` of that branch has finished and
  returned the new tip (or a later one). It includes that fetch. A reader that
  doesn't see the tip retries until `--visibility-timeout-secs`; then the push
  counts as `unseen` and as an error.
- `reader_queue` is how long an acknowledged push waited for a reader. Readers
  are started per push up to `--max-readers` (default 4 per writer). If this
  number is large, the load generator is the bottleneck, not the server.
- Clones are `--bare`, so checkout time on the client is not included.
- Received bytes are the sizes of the packs the clients stored (clients keep
  every received pack as a pack, `transfer.unpackLimit=1`), so
  `received_bytes_per_fetch` is the bytes served per fetch that the depth-1
  storm reports against stock `git upload-pack`.
- Client-side housekeeping is off (`gc.auto=0`, no commit-graph writes), and
  every client is isolated from the machine's Git configuration.
- `wall_seconds` covers the measured phase only, not setup.

Percentiles use the nearest-rank method, so every reported percentile is a
latency that was actually measured.

## Result format

One JSON document per run, schema `aspen-bench.result/v1`:

```json
{
  "schema": "aspen-bench.result/v1",
  "workload": "writers",
  "target": { "label": "gitea", "url": "http://127.0.0.1:3000/bench/bench-writers-1793683500.git" },
  "params": { "writers": 16, "duration_secs": 30.0, "max_readers": 64, "protocol": 2,
              "repo_files": 200, "repo_seed": 1, "...": "every knob that shapes the load" },
  "environment": { "git_version": "git version 2.43.0", "cpus": 4, "memory_bytes": 16772120576,
                   "cpu_model": "...", "kernel": "...", "aspen_commit": "...", "ci": "github-actions" },
  "started_at_unix_ms": 1793683500359,
  "wall_seconds": 30.4,
  "latencies_ms": {
    "push": { "count": 480, "min": 58.1, "mean": 127.9, "p50": 131.9, "p90": 162.4, "p99": 181.3, "max": 190.0 }
  },
  "throughput": { "pushes_per_second": 15.8 },
  "counts": { "pushes_ok": 480, "pushes_failed": 0, "seen": 480, "unseen": 0 },
  "errors": { "count": 0, "samples": [] }
}
```

A summary with no samples has `null` percentiles, never zeros. `errors.samples`
keeps the first 20 messages; `errors.count` is exact.

## Baselines and the regression check

A baseline is a stored result. `aspen-bench compare` checks a run against
one:

```bash
aspen-bench compare --baseline old.json --current new.json
```

It exits 1 if a metric regressed beyond its tolerance, a checked metric is
missing, or the two runs used different workloads or parameters (pass
`--allow-incompatible` to compare anyway). [tolerances.json](tolerances.json)
holds the rules; it is built into `aspen-bench` as the default, and
`--tolerances <file>` replaces it. Each rule gives, per metric key, which
direction is better, the allowed relative regression in percent, and an
absolute slack in the metric's unit.
Metric keys look like `latencies_ms.push_to_fetch.p99`,
`throughput.pushes_per_second` or `errors.count`; `*` matches one segment,
and the first matching rule wins. The committed tolerances are loose on
purpose: shared CI runners are noisy, and the check is meant to catch large
regressions, not to rank servers.

Stored baselines live in `bench/baselines/results/<machine-class>/<target>/`.
The repository stores nightly-profile baselines for `local` and
`git-http-backend` on `github-ubuntu-24.04` (2 vCPUs, 8 GB). Gitea, Forgejo
and GitLab have none on purpose: a regression there would only show drift in
a third-party image or the runner, not in Aspen. The `aspen` target runs
nightly; its baselines are recorded from those runs on the nightly machine
class (shared runners differ from the campaign machine, so the campaign's
numbers are not baselines).
`run.sh <target> check` compares the results of the last run with them, using
`BENCH_ENV` as the machine class (`local` by default, `github-ubuntu-24.04` in
the nightly job). Only compare runs from the same machine class. To record a
new baseline, run the profile on that machine class and copy the JSON files
there in a pull request that says why the numbers changed.

## The baseline servers

| Target | What runs | Version |
|---|---|---|
| `git-http-backend` | Stock `git http-backend` as CGI behind Apache (event MPM, up to 1,024 concurrent requests), configured as in the git-http-backend(1) manual | Debian bookworm's Git |
| `gitea` | Gitea with Postgres 17 | `gitea/gitea:28.3.0` |
| `forgejo` | Forgejo with Postgres 17 | `codeberg.org/forgejo/forgejo:17.0.1` |
| `gitlab` | GitLab CE Omnibus: nginx, Workhorse, Gitaly, Rails | `gitlab/gitlab-ce:19.5.3-ce.0` |
| `aspen` | `aspen dev` built from this checkout in release mode (SQLite and a local blob store, loopback only; no Docker), one repository per workload from `aspen repo create` | This checkout |
| `aspen-no-compaction` | The same with `aspen dev --no-background-worker` (no compaction, commit indexing or garbage collection) | This checkout |

### Benchmarking Aspen: branch-scoped pushes

Aspen advertises only trunk to a push on the plain repository URL (spike S2,
[docs/PLAN.md](../docs/PLAN.md) section 4.2). Stock `git push` then builds
every push to a benchmark branch against trunk and resends all the commits
the branch has gained, so the writers workload would measure ever-growing
packs rather than Aspen. Against Aspen, always pass `--branch-scoped-push`:
each writer (and the fetch-storm pusher) then pushes through its branch's
scoped URL, `<url>/-/refs/heads/<branch>`, where Aspen advertises trunk plus
that branch. The recovery `ls-remote` after a failed push and the cleanup
deletes use the same URL. The `aspen` target sets it for you through
`TARGET_PUSH_ARGS` in [baselines/aspen/target.sh](baselines/aspen/target.sh).

The option is recorded in the result as `"push_url": "branch-scoped"`, and
`compare` refuses to compare a branch-scoped run with a plain-URL run. The
other baselines keep pushing to the plain URL; they advertise every branch.

Readers always fetch from the plain URL. They name the benchmark branch
they want, which reaches Aspen as a protocol v2 `ls-refs` `ref-prefix`, so
they can fetch branches the server does not advertise. That needs protocol
v2: with `--protocol 0` or `1` the readers cannot see benchmark branches on
Aspen.

Override an image with `GITEA_IMAGE`, `FORGEJO_IMAGE` or `GITLAB_IMAGE`, and
a host port with `BENCH_PORT`. Every server listens on `127.0.0.1` only. The
scripts create a benchmark user with a random password (GitLab: a personal
access token) and keep it in `bench/baselines/.state/`, mode 600 and ignored
by Git, until `down`.

### GitLab and Gitaly

The plan asks for a GitLab Gitaly baseline. Gitaly is GitLab's Git storage
service, but it only speaks gRPC. Git over HTTP in GitLab is served by
Workhorse, which asks Rails to authorize every request (`/api/v4/internal`
pre-authorization) and then streams the Git data to and from Gitaly. A
"standalone Gitaly plus HTTP front" would mean re-implementing that
handshake or patching Workhorse, and the result would not be what any GitLab
user runs. The `gitlab` target therefore runs the Omnibus image, which is the
real production path (nginx → Workhorse → Gitaly, with Rails authorization),
with unrelated services such as the registry, Pages, KAS and monitoring
turned off and Git basic-auth throttling disabled. Expect several minutes to
boot and about 4 GB of memory.

### Profiles

`BENCH_PROFILE` picks the load:

| Profile | writers | fetch-storm: warm / cold / depth 1 (jobs × rounds) | clone | stale-branch: N (fetches each) | Repository |
|---|---|---|---|---|---|
| `smoke` (default) | 4 writers × 5 pushes | 8 × 2 / 4 × 2 / 8 × 2 | 2 clients, 4 clones | 10, 100 (2) | small (clone: small) |
| `nightly` | 16 writers for 30 s | 64 × 3 / 16 × 2 / 64 × 2 | 4 clients, 12 clones | 10, 100, 1,000 (3) | small (clone: medium) |
| `full` | 1,000 writers for 120 s | 1,000 × 3 / 100 × 2 / 1,000 × 3 | 16 clients, 64 clones | 10 to 10,000 (5) | medium (clone: large) |
| `storm1000` | 16 writers for 30 s | 1,000 × 3 / 100 × 2 / 1,000 × 3 | 4 clients, 12 clones | 10, 100, 1,000 (3) | small |
| `real` | 16 writers for 30 s | 1,000 × 2 / 8 × 2 / 1,000 × 2 | 4 clients, 8 clones | 10, 100, 1,000 (3) | `BENCH_REPO_SOURCE` |

`BENCH_WORKLOADS` runs a subset (space-separated names: `writers
fetch-storm fetch-storm-cold fetch-storm-depth1 clone stale-branch`).

The `full` profile starts thousands of `git` processes at once; raise
`ulimit -n` and `ulimit -u` first, and run the load generator on its own
machine.

### Interpreting numbers

In `run.sh` the server and the load generator share one machine, and every
simulated client is a real `git` process. On a small machine the client side
can dominate: watch `reader_queue`, CPU usage, and errors before drawing
conclusions. Comparisons between servers are only fair on identical
instances with the client on separate hardware, which is how D2 will run
them.

## Nightly job

The `bench` job in [.github/workflows/nightly.yml](../.github/workflows/nightly.yml)
runs the `nightly` profile against `local`, `git-http-backend`, `gitea`,
`forgejo`, `aspen` and `aspen-no-compaction` (and `gitlab` when the workflow
is started by hand), prints every result to the log, uploads them as
artifacts, and runs `run.sh <target> check` against the stored baselines for
`github-ubuntu-24.04`. The `local` entry also runs the ignored >2 GiB `.idx`
comparison with `git index-pack` (`aspen-pack`,
`a_pack_over_2_gib_indexes_byte_identical_to_git`), even when its baseline
comparison fails. Required CI only runs the seconds-long smoke in
`bench/tests/cli.rs` against `--url local`.

Cost: D2 added two matrix entries (`aspen`, `aspen-no-compaction`), the
`stale-branch` workload on every entry (1,000 trunk pushes and 9 fetches in
the `nightly` profile), a background-job wait on the two Aspen entries, and
the `.idx` test on `local`. Measured on
Nightly run 41787717247
(started by hand on `main` at `17a33be`, 2026-11-04; GitHub's `ubuntu-24.04`
runners, 2 vCPUs and 7.9 GB of memory), every entry passed:

| Entry | Job | Where the time went |
|---|---|---|
| `local` | 5.9 min | workloads 1.9 min (`stale-branch` 33 s); the `.idx` step 3.7 min (the test itself 172 s) |
| `git-http-backend` | 3.0 min | `stale-branch` 34 s |
| `gitea` | 10.9 min | `stale-branch` 8.2 min |
| `forgejo` | 15.5 min | `stale-branch` 11.0 min |
| `aspen` | 10.8 min | release build of `aspen` 7.6 min; `stale-branch` with the background-job wait 97 s |
| `aspen-no-compaction` | 10.2 min | release build of `aspen` 7.5 min; `stale-branch` 65 s |
| `gitlab` (manual runs only) | 15.9 min | boot 5.2 min; `stale-branch` 7.7 min |

The six scheduled entries add up to about 56 runner-minutes, against about
10 on the last scheduled run before D2
(a CI run:
`local` 1.7, `git-http-backend` 2.1, `gitea` 2.7, `forgejo` 3.2 min), so D2
added about 47 runner-minutes per night. The 1,000 trunk pushes of
`stale-branch` against the Docker targets and the Aspen release builds are
most of it.

Each matrix entry has a 45-minute timeout. The slowest entry took 16
minutes, so an entry can run almost three times slower than in this run
(a busy runner, a slower Docker image pull, a cold Rust cache) before it
times out, and a hung workload still fails within the hour. If an entry
ever takes more than about 35 minutes, split it (the `.idx` step into its
own job, or `stale-branch` on the Docker targets) rather than raising the
timeout.
