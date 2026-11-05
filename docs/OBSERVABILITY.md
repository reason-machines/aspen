# Observability

How to watch an Aspen installation: Prometheus metrics, logs, OpenTelemetry
traces, the shipped Grafana dashboard and alert rules, and `aspen doctor
jobs` for the background job queue. This is node C5 of
[PLAN.md](PLAN.md).

| What | Where |
|---|---|
| Metrics | `GET /-/metrics` on the server's **metrics listener**, Prometheus text format 0.0.4 |
| Logs | stderr: text, or JSON lines with `ASPEN_LOG_FORMAT=json` |
| Traces | OTLP/HTTP to the collector in `OTEL_EXPORTER_OTLP_ENDPOINT` (off when unset) |
| Dashboard | [deploy/observability/grafana/aspen.json](../deploy/observability/grafana/aspen.json) |
| Alert rules | [deploy/observability/prometheus/aspen-alerts.yml](../deploy/observability/prometheus/aspen-alerts.yml) |
| Job queue | `aspen doctor jobs` |

## Metrics

### Where they are served

`/-/metrics` is unauthenticated, as Prometheus expects, and it shows load,
error rates and repository counts. So it is **not** served on the Git
listener, which is the public endpoint. A server exposes it on a listener
of its own (`aspen_server::Server::metrics_router`), which only the
monitoring network should reach. On that listener every other path is
`404`. On Kubernetes, the Helm chart's `metrics.networkPolicy` admits only
the scrapers you name to that port (`deploy/README.md`).

Where the Git listener is private anyway (`aspen dev` on a laptop),
`ServerConfig::metrics_on_git_listener = true` also serves `/-/metrics`
on the Git router.

The database gauges below are read only when the server is built with
`Server::with_store_gauges(refresh)`. They are read when `/-/metrics` is
scraped, at most once per `refresh` per instance (30 seconds is a good
value); concurrent scrapes share one read. A read that takes longer than
`ServerConfig::store_gauges_timeout` (5 seconds) is abandoned and counts as
a failure, so a hung database delays a scrape by at most that long.

### Aggregating across instances

Counters and histograms are per process and start at zero when it starts:
`sum` them across instances, and use `rate` or `increase`. The database
gauges (`aspen_jobs*`, `aspen_trunk_repos`, `aspen_meta_stats_timestamp_seconds`)
describe shared state, so every instance reports the same value: use `max`,
never `sum`.

### Reference

Every family starts with `aspen_`. A test renders the endpoint and checks
that the dashboard and alert rules use only these names.

**Paging.** Each of these has an alert rule with `severity: page`.

| Metric | Type | Meaning |
|---|---|---|
| `aspen_jobs{kind,state}` with `state="dead"` | gauge | Background jobs parked as dead after 10 failed or expired attempts. Nothing retries them. See [Dead jobs](#dead-jobs) |
| `aspen_alert_stored_pack_forward_base_total` | counter | Fetches that hit a stored pack whose delta comes before its base. The fetch fails, and a `rewrite_pack` job is enqueued for the pack |
| `aspen_alert_store_corruption_total` | counter | Fetches that read stored data not matching its metadata row (a missing or short blob, a wrong checksum) |
| `aspen_alert_express_corrupt_pack_total` | counter | Express reads of a pack that does not hash to the checksum in its key ([EXPRESS.md](EXPRESS.md)) |

**Database gauges** (`Server::with_store_gauges`).

| Metric | Type | Meaning |
|---|---|---|
| `aspen_jobs{kind,state}` | gauge | Jobs that are not done, for every kind (`index_commits`, `compact`, `mirror_push`, `rewrite_pack`) and state (`queued`, `running`, `dead`), zeros included |
| `aspen_jobs_queued_lag_seconds{kind}` | gauge | How long the oldest due queued job of the kind has waited; 0 when none is due |
| `aspen_trunk_repos{closure}` | gauge | Repositories by the closure of their current trunk version, `exact` or `wider`. See [Wider trunk versions](#wider-trunk-versions) |
| `aspen_meta_stats_up` | gauge | 1 if the last read of these gauges succeeded. While it is 0 they keep their last values |
| `aspen_meta_stats_failures_total` | counter | Reads that failed or timed out |
| `aspen_meta_stats_timestamp_seconds` | gauge | When they were last read (Unix time) |

**Pushes** (the receive-pack service).

| Metric | Type | Meaning |
|---|---|---|
| `aspen_pushes_total{result}` | counter | Push requests by how they ended: `reported` (every ref got a status), `unpack_failed` (the pack was refused), `fatal` (the request did not parse), `store_error` (answered `503`, retryable). Git's empty probe before a large push is not counted |
| `aspen_push_duration_seconds` | histogram | From the first byte of the request to the report |
| `aspen_push_received_bytes_total` | counter | Pack bytes received |
| `aspen_push_ref_updates_total{result}` | counter | Ref commands by final status: `ok` (updated, or a no-op), `push_options_invalid`, or the reason it was refused, in snake case (`non_fast_forward`, `cas_lost`, `permission_denied`, `stale_old`, ...; one label per `aspen_core::error::RejectReason`) |
| `aspen_push_trunk_versions_total{closure}` | counter | Trunk versions committed, `exact` or `wider` |
| `aspen_push_branch_versions_total{closure}` | counter | Versions of every other ref committed, `exact` or `wider`. A `wider` one makes incremental fetches of that ref send more than they need; a write that depends on a `wider` version is `wider` too, so after a lost compare-and-swap across a forced update makes a branch version `wider`, later plain-URL fast-forwards of that branch stay `wider` until a forced update or a scoped-URL push that does not depend on it (issue #203) |
| `aspen_push_grace_exceeded_total` | counter | Pushes refused because garbage collection deleted their pack before they committed: registering to committing took longer than `gc_grace` (2 hours). Logged as a `warn`; the client retries and the push registers its pack again |
| `aspen_push_stage_seconds_total{stage}` | counter | Time in each push stage (`search`, `receive`, `gate_wait`, `prefetch`, `complete`, `graph`, `check`, `hook`, `upload`, `commit`; `search` is computing the packs a push searches, `hook` the push hook's checks with their range walks), summed over pushes |
| `aspen_push_prefetch_slots` | gauge | Pushes the prefetch gate admits at once |
| `aspen_push_prefetch_slots_in_use` | gauge | Slots held now |
| `aspen_push_prefetch_slots_peak` | gauge | Most slots held at once since the process started |
| `aspen_push_prefetch_slots_acquired_total` | counter | Slots granted |
| `aspen_push_prefetch_wait_timeouts_total` | counter | Waits for a slot that gave up; each is a retryable push error |

**Fetches** (the upload-pack service). The names are the ones node B2
introduced, unchanged.

| Metric | Type | Meaning |
|---|---|---|
| `aspen_fetches_total` | counter | Fetches planned |
| `aspen_fetch_errors_total` | counter | Fetches that ended in an `ERR` or a band-3 error |
| `aspen_fetch_plan_visited_versions` | histogram | Ref versions the planner's walk expanded per fetch (`PlanStats.visited`). Linear in versions until compaction's have-side skip lands (PLAN, fetch path step 2) |
| `aspen_fetch_plan_loaded_versions` | histogram | Ref versions read from the metadata store to plan one fetch, one read each (the wants', the haves' and those the loader fetched on demand). Counts ref versions only, not cover or pack-row reads, so it does not show the whole metadata cost of planning. D2 measured it at a few times `visited` for small walks and about `visited` plus the branch age for a stale branch without compaction (bench/campaign/README.md); much larger means the loader, not the walk, dominates planning |
| `aspen_fetch_plan_chain_skips_total` | counter | Cover chains the have-side walk skipped through (`PlanStats.chain_skips`); zero while no chained covers exist |
| `aspen_fetch_plan_duration_seconds` | histogram | Time to load the version graph and plan one fetch |
| `aspen_fetch_plan_needed_versions_total` | counter | Versions reachable from a want and not from a have, summed |
| `aspen_fetch_packs_planned_total` | counter | Packs the planner selected |
| `aspen_fetch_packs_uri_total` | counter | Packs sent as packfile URIs |
| `aspen_fetch_packs_stitched_total` | counter | Packs stitched into the inline pack |
| `aspen_fetch_stitched_bytes_total` | counter | Bytes of stitched packs sent |
| `aspen_fetch_haves_used_total` | counter | Client haves that stood for a ref version |
| `aspen_fetch_haves_dropped_total` | counter | Client haves dropped (unknown, not a tip, or a wider version) |
| `aspen_job_enqueue_failures_total` | counter | `rewrite_pack` jobs the fetch path could not enqueue; the next fetch that hits the pack tries again |

**Exact shallow packs** (node C8, on when `ASPEN_SHALLOW_PACKS` is on). A
`git fetch --depth=N` of a ref version's tip is served the pack that holds
exactly what that depth needs, built once per version and depth; see
`aspen_server::upload_pack::exact`. Fetches served this way are not
planned, so they are not in `aspen_fetches_total`.

| Metric | Type | Meaning |
|---|---|---|
| `aspen_shallow_exact_fetches_total{pack}` | counter | Shallow fetches served exactly: `recorded` (every pack already built), `built` (waited for a build), `closure` (a want's full closure was already exact) |
| `aspen_shallow_fallbacks_total{reason}` | counter | Shallow fetches that could have used exact packs and were sent the full history instead: `not_a_tip`, `no_build_slot`, `recently_failed`, `wait_timed_out`, `build_recorded_nothing`. A steady `wait_timed_out` or `no_build_slot` rate during fetch storms means builds are too slow or too few for the traffic |
| `aspen_shallow_builds_total{result}` | counter | Builds: `recorded`, `not_needed` (the closure is already exact), `too_large` (over the source limit), `not_a_commit`, `grace_exceeded` (the build took longer than `gc_grace`, so its pack was garbage-collected before it was recorded; logged), `over_limit` (the pack alone is larger than `ASPEN_SHALLOW_MAX_REPO_BYTES`, so it was not stored and the fetch got the full history; logged; issue #108), `failed` (logged; corruption also counts in `aspen_alert_store_corruption_total`) |
| `aspen_shallow_build_duration_seconds` | histogram | Time to download the closure, build, verify, upload and record one exact pack |
| `aspen_shallow_limit_evictions_total` | counter | Recordings evicted, least recently used first, to keep a repository's exact packs under `ASPEN_SHALLOW_MAX_REPO_BYTES` (issue #108). A steady rate means the cap is too small for what clients fetch: every eviction is a later rebuild |

**Client addresses** (the access layer, issue #121). Behind proxies
(`ASPEN_TRUSTED_PROXY_HOPS` above 0), a request whose forwarded client
address cannot be read is limited by its TCP peer instead.

| Metric | Type | Meaning |
|---|---|---|
| `aspen_client_address_fallbacks_total{reason}` | counter | Requests keyed by their TCP peer: `untrusted_peer` (the peer is not in `ASPEN_TRUSTED_PROXIES`: something reaches Aspen around its proxy, or the list misses a proxy), `missing` (fewer `X-Forwarded-For` entries than hops: the hop count is too high), `malformed` (the entry read is not an address). Always 0 without proxy hops. A steady rate means callers share their proxy's rate limits and failed-authentication bucket |

**Build.** `aspen_build_info{version}` is always 1.

**Express and CDN** (`aspen_express_*`): on servers that sign express URLs
or mount the express route, and on standalone `aspen express` nodes. See
[EXPRESS.md](EXPRESS.md#metrics).

### Dead jobs

A job (indexing a push's commits, compaction, a mirror push, rewriting a
pack) that fails or loses its lease 10 times is parked as `dead`. Nothing
retries it, and it keeps its slot in the queue, so its work does not happen
until someone acts: a dead `rewrite_pack` leaves a broken pack failing every
fetch that stitches it. Any dead job pages (`AspenDeadJobs`).

```console
$ DATABASE_URL=postgres://aspen@db.internal/aspen PGPASSWORD=... aspen doctor jobs
KIND             QUEUED  RUNNING     DEAD  OLDEST DUE
rewrite_pack          0        0        1  -

1 dead job(s):
  job 17: rewrite_pack pack 9 in acme/mono, 10 attempts
    last error: lease expired

Fix the cause first (the last error says what failed), then either:
  aspen doctor jobs requeue <id>   retry the work as a fresh job
  aspen doctor jobs resolve <id>   mark it done, if the work was done some other way
```

- `aspen doctor jobs` (or `aspen doctor jobs list`) lists jobs that are not
  done per kind and state, and every dead job (oldest first, `--limit`,
  default 100) with its repository, target, attempts and last error.
  `--json` prints the same as JSON.
- `aspen doctor jobs requeue <id>` marks the dead job done and enqueues a
  fresh job for the same kind and target, in one transaction. The fresh job
  has a new ID and starts at attempt 0. The dead row is never reset in
  place: workers fence every update on the job ID and attempt, so reusing
  attempt numbers on one row would let a stale worker finish the new attempt
  ([SCHEMA.md](../crates/aspen-meta/SCHEMA.md), "jobs").
- `aspen doctor jobs resolve <id>` marks the dead job done and enqueues
  nothing.

The database is the one the server uses, given as `--database-url` or
`DATABASE_URL` (for Postgres without a password, which comes from
`PGPASSWORD`, as for the server). The command never runs migrations. Exit
status: 0 on success (for the listing: and no job is dead), 1 when the
listing found dead jobs, 2 on any error, including requeueing or resolving
a job that is not dead. Requeues and resolves are logged at `info`.

### Wider trunk versions

Each ref version records whether its closure is exactly what its tip
reaches. A wider closure is inherited: once a trunk version is wider, every
later trunk version and every branch built on trunk is wider too, and every
incremental fetch of the repository becomes a full clone (PLAN, decisions
log, N1 review). Trunk stays exact in ordinary use, so two signals watch
for it:

- `aspen_trunk_repos{closure="wider"}` counts repositories whose **current**
  trunk version is wider (alert `AspenWiderTrunk`, a ticket);
- `aspen_push_trunk_versions_total{closure="wider"}` counts pushes that
  committed one, as they happen, and each logs a `warn` line inside its
  `push` span naming the repository (alert `AspenWiderTrunkCommitted`).

## Logs

The server and the CLI log through [`tracing`](https://docs.rs/tracing).
The `aspen` binary writes them to stderr.

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_LOG` | `info` | What to log, in `tracing`'s filter syntax: `info`, `aspen_server=debug,info`, ... |
| `ASPEN_LOG_FORMAT` | `text` | `text`, or `json` for one JSON object per line (with the current span's fields) |

To debug, raise the level for Aspen's own targets only, for example
`ASPEN_LOG=info,aspen_server=debug` or `ASPEN_LOG=info,aspen=trace`
(`aspen` matches every `aspen_*` crate). A bare `ASPEN_LOG=debug` or `trace`
also turns on the libraries' logs (the HTTP stack, the database driver, the
S3 client): very verbose, and some of them log request details Aspen itself
never logs.

Events worth knowing:

| Level | Message | When |
|---|---|---|
| `info` | `git request` | Every request to the Git listener's Git router, those the access guard refuses (`401`, `403`, `404`, `429`) included (an access log): `status`, `ms`, inside the `git_request` span (`method`, `path`, and, once the guard admits it, `repo`, `endpoint`, `scope`). The path is logged without its query string, and no header is logged. `/-/blobs/...`, `/-/express/...` and `/-/metrics` are not in it |
| `info` | `push answered` | Every push: `result`, `received_bytes`, total and per-stage milliseconds, inside the `push` span |
| `info` | `pack refused` | A push whose pack was refused, with the reason |
| `warn` | `committed a wider trunk version: ...` | See [Wider trunk versions](#wider-trunk-versions) |
| `warn` | `push failed with a storage error` | The push was answered `503` |
| `info` | `fetch failed` | A fetch that ended in an error (other than the paging kinds below) |
| `error` | `a stored pack is not base-first; ...` | The forward-base page, with the pack ID |
| `error` | `cannot enqueue rewrite_pack` | The repair job for that pack could not be queued |
| `error` | `stored data does not match its row` | The corruption page |
| `warn` | `reading metadata gauges failed` | `aspen_meta_stats_up` is 0 (the message ends in `timed out` when the read took too long) |
| `debug` | `fetch planned` | `visited`, `chain_skips`, `loaded_versions`, `needed`, `packs`, haves used and dropped, `plan_ms` |

Logs never contain credentials, request bodies or object contents.

## Traces

When `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`)
is set, the binary exports its spans with OTLP over HTTP (protobuf),
batched on a background thread and flushed when the command exits. The
standard OpenTelemetry variables apply: `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_SERVICE_NAME` (default `aspen`),
`OTEL_RESOURCE_ATTRIBUTES`; `OTEL_SDK_DISABLED=true` or
`OTEL_TRACES_EXPORTER=none` turns export off. Every span carries
`service.version`. `ASPEN_LOG` filters spans too: a span below the level
is neither logged nor exported.

Only `http/protobuf` is supported (usually port 4318). An
`OTEL_EXPORTER_OTLP_PROTOCOL` naming anything else stops the binary at
startup with an error, rather than being ignored. Metrics are not exported
over OTLP: an OpenTelemetry Collector that should forward them scrapes
`/-/metrics` with its `prometheus` receiver.

Spans: `git_request` (every request to the Git router), `push` (inside it, for a push),
`doctor_jobs`.

## Dashboard and alerts

- **Grafana:** import
  [deploy/observability/grafana/aspen.json](../deploy/observability/grafana/aspen.json)
  and pick the Prometheus data source. A `job` variable selects scrape jobs.
- **Prometheus:** add
  [deploy/observability/prometheus/aspen-alerts.yml](../deploy/observability/prometheus/aspen-alerts.yml)
  to `rule_files`. Rules are labelled `severity: page` or
  `severity: ticket`; route them in Alertmanager.

A scrape configuration for a server whose metrics listener is on port 9464:

```yaml
scrape_configs:
  - job_name: aspen
    scrape_interval: 30s
    static_configs:
      - targets: ["aspen-1.internal:9464", "aspen-2.internal:9464"]
```

## For the binary that serves Git

`aspen serve` and `aspen dev` (node C6) wire this up as follows:

1. Hold the guard from `telemetry::init()` (in `aspen-cli`) for the life of
   the process, so logs and traces are configured as above.
2. Build the server with `Server::with_store_gauges(Duration::from_secs(30))`.
3. Serve `Server::router` on the Git listener and `Server::metrics_router`
   on a separate, private listener. `aspen dev` may instead set
   `ServerConfig::metrics_on_git_listener`.
