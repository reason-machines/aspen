# Item 10 live load, 2026-11-06/07: raw results

Raw worker and monitor output from the five launches against
`place.reasonmachines.com` (`cbc87ab`, task definition `aspen-demo-place:24`)
summarised in `docs/PLAN.md` (decisions log, "Item 10 result"). Each `runN/`
holds:

- `results/api-*.json`, `results/git-*.json`: one result object per worker, as
  `place_load.py` / `git_load.py` printed it (`{"result": ...}`).
- `stderr/*.txt`: the last 4 KB of a worker's stderr, only where non-empty.
- `monitor.json`: `monitor.py` output for the run (page, API and SVG polls, live
  feed, merge lag). Recorded before the resync handling was added, so these
  files have no `resyncs` field, their merge lag is not resync-safe (run5's single
  stream connection means no resync could have occurred there), and their
  `max_seconds_between_events` excludes the silence before the first and after
  the last event.
- `aggregate-api.json`, `aggregate-git.json`: `aggregate.py` output at the time.
- `start`: the common start (Unix seconds).

Recompute an aggregate:

```bash
cd bench/throughput
python3 aggregate.py --expected-workers 12 results/item10-2026-11-07/run5/results/api-*.json
python3 aggregate.py --expected-workers 3 results/item10-2026-11-07/run5/results/git-*.json
```

| Run | Start (UTC) | What it is | Usable |
|---|---|---|---|
| run1 | 23:09 | git only: 3 x 67/s; the 8 API workers all failed `schedule_and_drain_do_not_fit_deadline` (start lead plus schedule exceeded the 420 s deadline) | git: 99/s accepted |
| run2 | 23:19 | API only: 8 x 50/s; git workers `setup_missed_common_start` | API: 333/s accepted |
| run3 | 23:32 | API only: 12 x 45/s; API workers 4, 7, 10 `setup_transport_error` before sending (405/s scheduled on surviving workers, 121,500 hold slots; 18,154 skipped before sending); git workers failed on the deadline | API: 103,346 sent and accepted = 344.487/s (observed, not a ceiling) |
| run4 | 23:44 | 16 x 35/s API; git workers `deadline_exceeded` (full clone while ~1,550 load branches were live); `main` history reset mid-hold | partial |
| run5 | 00:02 | mixed: 12 x 45/s API (workers 2, 4, 5, 8 `setup_transport_error`) + 3 x 20/s git: 420/s scheduled on surviving workers (126,000 hold slots), 46,199 skipped before sending (45,896 API, 303 git: session or lane still busy), 79,801 sent | mixed: 266/s actually sent and accepted (207.013 + 58.990; observed average, not a proven server ceiling). Git read-back: advertised-tip only, 360/360 (`read_back_tip_matches` 120 per worker); no fresh-fetch object check (predates it) |

Launch manifests (they held presigned URLs) and the S3 readiness markers are not
included. Task CPU and memory (CloudWatch), the no-restart observation (ECS) and
personas staying on were read live during the runs; no artifact for them is
committed, so the PLAN labels them as observed.
