# Operations

How to run an Aspen installation day to day. The metric names, log fields
and alert rules are listed in full in [Observability](reference/observability.md).

## Health

| Check | Means |
|---|---|
| `GET /healthz` → `200` | The process serves HTTP. Use for liveness; it does not touch the database, so a database outage does not restart every instance at once |
| `GET /readyz` → `200` | The metadata database answers a query. Use for load balancer readiness |
| `aspen healthcheck --url http://127.0.0.1:7070/readyz` | The same, from inside the container (exit 0 only on `200`) |

On SIGTERM a server stops accepting connections and finishes the requests
in flight. Give it time to do so: a large clone can take minutes (the Helm
chart allows 300 seconds).

## Metrics and alerts

Point Prometheus at each instance's metrics listener (`ASPEN_METRICS_LISTEN`,
port 9464), import the Grafana dashboard and load the alert rules:

- dashboard: [deploy/observability/grafana/aspen.json](../../deploy/observability/grafana/aspen.json)
- alert rules: [deploy/observability/prometheus/aspen-alerts.yml](../../deploy/observability/prometheus/aspen-alerts.yml)

Counters are per process: `sum` them across instances. The database gauges
(`aspen_jobs`, `aspen_trunk_repos`) describe shared state, so every
instance reports the same value: use `max`.

The alerts that page all mean stored data needs a person:

| Alert | What it means | What to do |
|---|---|---|
| `AspenDeadJobs` | A background job failed 10 times and stopped retrying | [Dead jobs](#dead-jobs) below |
| `AspenStoredPackNotBaseFirst` | A stored pack has a delta before its base; fetches that use it fail | A `rewrite_pack` job is queued automatically; check that it does not die |
| `AspenStoreCorruption` | A blob does not match its database row (missing, short, wrong checksum) | Check the bucket object of the pack named in the error log against its `packs` row |
| `AspenExpressCorruptPack` | An express node read a pack that does not hash to the checksum in its key | As above; the express node streams it but never caches it |

The other rules open tickets: a repository whose trunk became **wider**
(`AspenWiderTrunk`; fetches stay correct but send more, see
[Architecture](architecture.md#exact-and-wider-versions)), a lagging job
queue, database gauges that cannot be read, an express endpoint that is
down, and others listed in the rules file. Open mirror conflicts show in
the gauge `aspen_alert_mirror_conflicts`; the shipped rules do not alert on
it yet, so add a rule if you run mirrors in `async` mode.

## Dead jobs

Indexing large pushes, compaction, mirror pushes and pack repairs run as
jobs in the database. A job that fails or loses its lease 10 times is parked
as `dead`, and nothing retries it.

```bash
aspen doctor jobs                 # counts per kind and state, and every dead job with its last error
aspen doctor jobs requeue <id>    # after fixing the cause: run it again as a fresh job
aspen doctor jobs resolve <id>    # the work was done some other way: mark it done
```

`aspen doctor jobs` exits 1 when it finds dead jobs, so it also works as a
cron check. It reads `DATABASE_URL` and `PGPASSWORD` and never migrates.

## Repositories, tokens and keys

```bash
aspen repo create acme/app                         # trunk refs/heads/main
aspen repo create acme/tools --trunk refs/heads/trunk --allow-force none
aspen token create --subject agent:ci-7 --grant write:acme/app
aspen token list
aspen token revoke <id>
aspen key add ~/.ssh/id_ed25519.pub --subject user:alice --grant write:acme/*
aspen key list
aspen key revoke SHA256:...
```

`--allow-force` is the repository's force policy: `none`, `branches` (the
default; never trunk) or `all`. A revoked token or key is refused on the next
request, including on an SSH connection that is already open. Each platform
has its own way to run these commands next to the server (`docker compose
exec`, `kubectl exec`, ECS Exec, a Cloud Run job); the
[deployment reference](reference/deploy.md) lists them.

## Background work

Each `aspen serve` instance runs:

- **Compaction**: every 16 versions of a ref, packs are merged into a cover
  ([Architecture](architecture.md#compaction)). It needs room under
  `<ASPEN_SPOOL_DIR>/compact` for about twice the largest cover.
- **Commit indexing** for pushes too large to index while they commit.
- **Garbage collection** of packs nothing names, after a 2-hour grace
  period, in a pass every 5 minutes.
- **Exact shallow packs**, built on the first `--depth=N` fetch of a version.
  Once a recording is 10 minutes old and its version is no longer current,
  it is dropped and its pack collected 2 hours later: within 2 hours 20
  minutes of the version being superseded. A pack no fetch has used for
  `ASPEN_SHALLOW_UNUSED_SECS` (24 hours) is dropped too, current tip or
  not, and each repository's packs stay under
  `ASPEN_SHALLOW_MAX_REPO_BYTES` (8 GiB), least recently used first; a
  dropped pack's blobs stay for the grace period, so no fetch streaming it
  is cut off. `aspen serve` refuses a grace period shorter than twice the
  longest packfile URL lifetime (1 hour).
  `ASPEN_SHALLOW_PACKS=false` turns them off without a release. A steady
  `aspen_shallow_limit_evictions_total` rate means the cap is too small for
  the depths and tips your clients fetch: each eviction is a rebuild later.
- **Mirror sync passes** every `ASPEN_MIRROR_SYNC_SECS`.

Watch `aspen_jobs_queued_lag_seconds` for a queue that falls behind.

## Mirrors

```bash
aspen mirror status                   # every mirror
aspen mirror status acme/app          # its refs in conflict and its last error
aspen mirror sync acme/app            # one sync pass now
aspen mirror resolve --keep upstream acme/app refs/heads/main   # or --keep aspen
```

In `async` mode, a branch that moved both in Aspen and upstream since they
last agreed is a **conflict**: Aspen writes neither side and raises a
paging gauge until someone picks a side. See [Mirror mode](reference/mirror.md).

## Upgrades

New binaries apply their database migrations at startup, and most upgrades
can roll one instance at a time. The exceptions are listed in the
[Upgrading](reference/deploy.md#upgrading) section of the deployment
reference; read it before every upgrade. Today there is one: servers built
before schema migration 0005 must all be stopped before a server with it
starts.

## Capacity

- **Spool disk**: about twice the largest push, plus compaction's working
  space.
- **Memory**: about 140 bytes per object of the largest push, plus
  `ASPEN_PREFETCH_MEMORY_BYTES` shared by concurrent pushes.
- **Rate limits are per instance.** With N instances, a caller can get up to
  N times the configured rate.
- **Object storage**: each push stores a pack, and compaction adds merged
  copies while keeping the originals (their versions still name them), so
  a repository takes more space than one packed by a classic server. How
  much more has not been measured on a large installation yet.
