# Benchmarks

The first benchmark campaign (node D2) compared Aspen with stock Git's own
server, `git http-backend`, on one 4-vCPU cloud machine, with a small
synthetic repository and with git.git at v2.47.0. The
[campaign write-up](reference/bench-campaign.md) has the method, every
table, the limits and the script that reruns it; the raw results and the
generated tables are in
[results/2026-11-04-cloud-4vcpu](../../bench/campaign/results/2026-11-04-cloud-4vcpu/REPORT.md)
in the repository.

The server and every client shared that one machine, Aspen ran as `aspen
dev` (SQLite and local disk), and the stock server ran behind a small test
bridge rather than Apache. The machine's memory was resized partway
through: the synthetic runs, the 1,000-job synthetic storms and Aspen's
git.git writers and storms had 8.35 GB; Aspen's git.git clone, every stock
git.git run and the 250- and 500-job threshold runs had 16.94 GB. Byte
counts are exact and do not depend on memory; treat latency and throughput
as indicative until the campaign runs on identical instances with the
clients on separate machines.

## Bytes against stock `git upload-pack`

| Fetch | Aspen / stock |
|---|---|
| Depth-1 clone of git.git (1,000 concurrent jobs) | 1.029 (12.47 MB against 12.12 MB) |
| Depth-1 clone, synthetic repository (1,000 jobs) | 1.000 |
| Full clone of git.git | 1.000 (252.8 MB) |
| One new commit on a warm client, git.git | 0.971 |

## The stale-branch fetch

A client current on trunk fetches a branch created N trunk pushes ago. With
compaction, the planner's walk grows roughly with log N (four points):

| N | Versions visited, with compaction | Without compaction | Planning time, with / without |
|---|---|---|---|
| 10 | 11 | 11 | 15.1 / 1.4 ms |
| 1,000 | 26 | 1,001 | 16.8 / 111 ms |
| 10,000 | 35 | 10,001 | 17.9 / 563 ms |

Compaction adds a planning floor of about 15 ms that is not explained yet:
at N = 10 both servers read the same 33 ref versions.

## Load

All runs on one machine, clients included. The git.git rows ran Aspen with
8.35 GB of memory and stock with 16.94 GB, except the clone (both 16.94 GB).

| Workload | Aspen | Stock |
|---|---|---|
| 1,000 warm jobs fetch one new commit, git.git (Aspen 8 GB, stock 16 GB) | p50 13.4 s | p50 39.6 s |
| 1,000 depth-1 jobs, synthetic repository | p50 5.1 s | p50 29.7 s |
| 16 writers for 30 s, git.git: push→fetch (Aspen 8 GB, stock 16 GB) | p50 198 ms, push p99 2,983 ms, 12.4 pushes/s | p50 1,563 ms, push p99 1,356 ms, 14.2 pushes/s |
| 4 clients clone git.git (both 16 GB) | p50 29.4 s | p50 32.9 s |

## Where Aspen loses

- **Push latency grows with trunk history.** Over 10,000 trunk pushes,
  Aspen's push p50 grew from 29 ms to 185 ms; stock stayed at 31 ms
  (#149).
- **Fetch storms can exhaust the metadata pool.** With 1,000 concurrent
  depth-1 fetches of git.git (8 GB machine), about half failed with
  "internal storage error; retry"
  (#150). Stock
  completed all 2,000 (p50 472 s); Aspen completed 961 (p50 51.6 s), so
  the latencies are not comparable. 250 and 500 jobs had no failures, but
  those runs had the 16 GB machine and a separately started server.
- **Push tail latency and push rate on git.git:** p99 2,983 ms against
  1,356 ms, 12.4 pushes/s against 14.2 (Aspen 8 GB, stock 16 GB).
- **A single stale-branch fetch is slower than stock:** 82 ms against
  33 ms at N = 10, 126 ms against 82 ms at N = 10,000.
- **Depth-1 packs of a real repository are 2.9% larger** than stock's.

## Reproducing

```bash
bench/campaign/run.sh /tmp/aspen-campaign
```

The [benchmark tool](reference/bench.md) page explains every workload and
how to run it against `git http-backend`, Gitea, Forgejo and GitLab.
