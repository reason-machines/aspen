# Configuration

`aspen serve` is configured only through environment variables, so every
deployment path (Compose, Helm, Terraform, a systemd unit) sets the same
ones. It checks them at startup and refuses to start, naming the variable,
when one is unusable: a database URL that carries a password, a policy that
does not parse, an SSH listener without a host key. `aspen serve --help`
prints a summary of the most-used variables with their defaults; this page
is the complete list.

This page lists every variable the `aspen` binary reads. A check in CI
(`site/build.py`) compares it with the code, so a new variable cannot ship
undocumented. The [deployment reference](reference/deploy.md#configuration-contract)
shows how each deployment path sets them.

## Server and storage

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_LISTEN` | `0.0.0.0:7070` | Address for Git over HTTP. Plain HTTP/1.1 and cleartext HTTP/2 on one port; put a TLS terminator (load balancer, ingress, Caddy) in front |
| `ASPEN_PUBLIC_URL` | unset | The URL clients use, such as `https://git.example.com`. Needed for signed `/-/blobs` URLs and for `/-/express` on this server |
| `DATABASE_URL` | required | `postgres://aspen@db:5432/aspen?sslmode=require`, or `sqlite:///var/lib/aspen/aspen.db?mode=rwc` for one machine. Must **not** contain the password |
| `PGPASSWORD` | unset | The PostgreSQL password. Kept out of the URL so a secret store can supply it on its own and it never appears in logs or process listings |
| `ASPEN_BLOB_URL` | required | Where packs are stored: `s3://bucket[/prefix]` for S3 and S3-compatible stores (MinIO, R2, GCS's S3 API), or `file:///path` for a local directory. S3 settings come from the usual `AWS_*` variables (`AWS_REGION`, `AWS_ENDPOINT_URL`, credentials, or the instance's role) |
| `ASPEN_BLOB_URL_SECRET` | unset | With a `file://` store only: a hex key of at least 32 bytes that signs `/-/blobs` download URLs, so clients can fetch packs over packfile URIs from this server. Needs `ASPEN_PUBLIC_URL` |
| `ASPEN_SPOOL_DIR` | `./spool` (`/var/lib/aspen/spool` in the image) | Where pushes are received and completed before upload, where the compaction worker repacks (`compact/`), where exact shallow packs are built (`shallow/`) and where mirror packs pass (`mirror/`). Use a real disk, never tmpfs, with room for about twice the largest push |
| `ASPEN_MIGRATE` | `true` | Apply database migrations at startup. Concurrent instances wait on the database's migration lock. `aspen migrate` does the same by hand |

## Access

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_POLICY_FILE` or `ASPEN_POLICY` | tokens only | The access policy, as a JSON file or inline JSON (not both) |
| `ASPEN_LIMITS_FILE` or `ASPEN_LIMITS` | no request rates (warned at startup) | Rate limits and push quotas, as a JSON file or inline JSON (not both). The compose stack, the Helm chart and the Terraform modules set read and push rates by default |
| `ASPEN_TRUSTED_PROXY_HOPS` | `0` | How many proxies in front of Aspen append to `X-Forwarded-For`. Anonymous callers, and every caller's failed sign-ins, are limited by the address the outermost trusted proxy recorded; `0` uses the TCP peer. A value higher than the real number of proxies lets clients choose their own address |
| `ASPEN_TRUSTED_PROXIES` | private ranges | Addresses or CIDR blocks (`10.0.0.0/16, 192.0.2.7`) the nearest proxy connects from. `X-Forwarded-For` is read only on connections from these, so a client that reaches Aspen directly is limited by its own address. The default trusts loopback, private, shared (`100.64.0.0/10`) and link-local addresses. A request whose forwarded address cannot be read is limited by its TCP peer and counted in `aspen_client_address_fallbacks_total` |

### Grants

API tokens, SSH keys, anonymous callers and OIDC identities all hold
**grants**, written `<permission>:<repositories>[:<refs>]`:

| Grant | Allows |
|---|---|
| `read:acme/app` | Clone and fetch `acme/app` |
| `write:acme/*` | Read and push every repository under `acme/` |
| `write:acme/app:refs/heads/agent-7/*` | Read `acme/app`; push only branches under `agent-7/` |
| `admin:*` | Everything, on every repository |

Reads are always repository-wide; only `write` grants can name refs. A
token or key holds exactly its own grants plus the anonymous ones. Create
them with:

```bash
aspen token create --subject agent:ci-7 --grant write:acme/app   # printed once
aspen token list
aspen token revoke <id>
aspen key add ~/.ssh/id_ed25519.pub --subject user:alice --grant write:acme/*
aspen key list
aspen key revoke SHA256:...
```

These commands read `DATABASE_URL` and `PGPASSWORD` like the server. The
subject is a label for logs and rate limits (`user:alice`, `agent:ci-7`).

### The access policy

Without a policy, every request needs a token or key. A policy can give
everyone some grants, and accept OIDC identity tokens from CI systems or an
identity provider:

<!-- docs-example: policy -->
```json
{
  "anonymous": ["read:public/*"],
  "oidc": [
    {
      "name": "gha",
      "issuer": "https://token.actions.githubusercontent.com",
      "audience": "https://git.example.com",
      "jwks": {
        "keys": [
          {
            "kty": "EC",
            "crv": "P-256",
            "kid": "example",
            "alg": "ES256",
            "use": "sig",
            "x": "UF7c6V6hz4GhiAJfNgXsulwjXLWAnHrS2WY1Z2Ny9vI",
            "y": "rF4cjWa6WUCvIQUGQ2hPUQCSWGcNigiGRrWmQ7eHdS8"
          }
        ]
      },
      "rules": [
        {
          "claims": { "repository": "acme/app", "ref": "refs/heads/main" },
          "grants": ["write:acme/app"]
        }
      ]
    }
  ],
  "subjects": {
    "oidc:gha:repo:acme/tools:ref:refs/heads/main": ["read:acme/*"]
  },
  "leeway_seconds": 60
}
```

- `anonymous`: grants every caller holds, signed in or not.
- `oidc`: accepted issuers. A caller sends the identity token as the HTTP
  password (or as a bearer token). Aspen checks the signature against
  `jwks` (RS256/384/512 and ES256/384; never `none` or HMAC), `iss`
  exactly, `aud` containing `audience`, `exp` and `nbf` with
  `leeway_seconds` of slack. Each rule whose claims all match adds its
  grants; a rule with no claims is refused, since it would match every
  token.
- `jwks` is the issuer's key set document, pasted in: Aspen does not fetch
  it from the issuer yet, so update the policy when the issuer rotates its
  keys. The key above is a placeholder; for GitHub Actions, use the document
  at `https://token.actions.githubusercontent.com/.well-known/jwks`.
- `subjects`: grants for one OIDC identity, by its principal ID,
  `oidc:<issuer name>:<sub claim>`.

### Rate limits and push quotas

<!-- docs-example: limits -->
```json
{
  "read": { "burst": 100, "per_second": 20 },
  "push": { "burst": 20, "per_second": 5 },
  "quota": {
    "max_pack_bytes": 1073741824,
    "max_objects": 1000000,
    "bytes_per_hour": 10737418240
  },
  "concurrent": { "fetches": 16, "pushes": 4, "stitches": 8 },
  "max_keys": 100000
}
```

- `read`, `push`: token buckets per caller (the principal when signed in,
  the client address when anonymous; IPv6 addresses count per /64). A
  caller over its rate gets `429`.
- `quota.max_pack_bytes`, `quota.max_objects`: per push. A push that
  declares a larger body (`Content-Length`) gets `413` before its body is
  read, and one that streams past the cap fails as soon as it crosses it;
  the object count is checked against the pack header before any object
  is read. Nothing of a refused push is stored.
- `quota.bytes_per_hour`: a rolling budget of pushed bytes per caller,
  charged as the push streams in.
- A push body sent gzip-compressed (JGit compresses every push) counts as
  the larger of its bytes as sent and its bytes inflated, for both the cap
  and the budget, so a small compressed body cannot unpack past either.
  The `413` check compares the declared length, which is the compressed
  size.
- `concurrent`: how many requests one caller may have running at once,
  over HTTP and SSH together, keyed like the rates. `fetches` counts
  `git-upload-pack` requests (each HTTP request while the server reads,
  plans and answers it; each SSH session for its whole length),
  `pushes` counts `git-receive-pack` requests and SSH sessions, and
  `stitches` counts inline packs being built and streamed. A request over
  a cap is refused at once: `429` with `Retry-After` over HTTP, a
  retryable `ERR` over SSH and for a stitch. The defaults are 16, 4 and
  8; `null` turns a cap off. The [instance limits](#admission) bound
  everyone together.
- `max_keys`: most callers each limiter tracks.

Every field is optional. The buckets live in each server's memory, so with
N servers behind a load balancer a caller can get up to N times the
configured rate.

## Throughput tuning

These settings decide how many pushes and fetches one instance handles at
once, and how fast each one moves. The defaults suit one instance with 4 to
8 vCPUs and 16 GiB of memory in front of a managed PostgreSQL and S3.
Every setting is per instance: with N instances behind a load balancer the
service runs up to N times as much, and the database sees N pools.

Change one setting at a time, against a measurement. To find what limits
push throughput, read `aspen_push_stage_seconds_total{stage}` on
`/-/metrics` ([Observability](reference/observability.md)): it adds up the
time pushes spend in each stage, so the stage that grows fastest under load
is the one to tune.

| Where push time goes | What it means | Settings to look at |
|---|---|---|
| Pushes answered `503` with `Retry-After` | Every push slot was taken and the queue was full or its wait ran out | [Admission](#admission) |
| Requests answered `503` saying `memory pressure` | The process's resident memory is over `ASPEN_MEMORY_HIGH_WATER_BYTES` | Lower the caches (`ASPEN_IDX_CACHE_BYTES`, `ASPEN_OBJECT_CACHE_BYTES`, `ASPEN_TRUNK_TREE_CACHE_BYTES`) or the concurrency limits, or give the instance more memory |
| `stage="gate_wait"`, or `aspen_push_prefetch_wait_timeouts_total` rising | Pushes wait for delta-base memory | `ASPEN_PREFETCH_MEMORY_BYTES`, `ASPEN_PREFETCH_FLOOR_BYTES` |
| `stage="prefetch"` | Reading delta bases from stored packs | `ASPEN_PREFETCH_PARALLEL_READS`, `ASPEN_OBJECT_CACHE_BYTES`, `ASPEN_IDX_CACHE_BYTES`, `ASPEN_IDX_WHOLE_MAX_BYTES` |
| `stage="complete"` | Completing and indexing the pack on local CPU | `ASPEN_PUSH_THREADS`, the instance's vCPUs |
| `stage="graph"` | Scanning the pushed pack, diffing its trees against their parents', and looking up what changed (connectivity, objects already stored) | `ASPEN_TRUNK_TREE_CACHE_BYTES`, `ASPEN_IDX_CACHE_BYTES`, `ASPEN_IDX_WHOLE_MAX_BYTES` |
| `stage="upload"` | Writing the pack to the object store | [Uploads](#uploads-to-the-object-store) |
| `stage="commit"` | The database transaction that moves the ref | [Metadata database](#metadata-database) (`ASPEN_GROUP_COMMIT`), `ASPEN_SYNC_COMMIT_ROWS` |
| The database's CPU is high while the servers' is not | Each push sends its statements to the database; `aspen_push_db_statements_total` counts them (on PostgreSQL about 4 per branch push) | [Metadata database](#metadata-database) (`ASPEN_GROUP_COMMIT` shares the `BEGIN` and `COMMIT` of concurrent pushes) |
| `aspen_jobs_queued_lag_seconds{kind="compact"}` growing | Compaction falls behind pushes | [Background work](#background-work) |

Aspen checks every value at startup. A value that is not a number, is out
of range, or does not fit with another setting stops the start with a
message that names the variable, such as
`ASPEN_PREFETCH_FLOOR_BYTES: must be at most ASPEN_PREFETCH_MEMORY_BYTES, the memory all pushes share`.
Sizes are in bytes and durations in the unit the name gives.

### Admission

These bound how much Git work one instance runs at once, whoever sends it
(issue #118). A request takes a slot before its body is read and gives it
back when it is done, however it ends: answered, failed, or abandoned by a
client that hung up. When every slot of its kind is taken, a request waits
in a queue; with the queue full, or after the wait, it is refused with
`503` and `Retry-After` over HTTP (Git prints `HTTP 503`), or a retryable
`ERR` over SSH and for a stitch. Callers are also capped one by one
(`concurrent` in the [limits](#rate-limits-and-push-quotas)).

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_MAX_CONCURRENT_FETCHES` | `32` | `git-upload-pack` requests (advertisement, `ls-refs`, each `fetch` round up to its pack) and SSH fetch sessions at once. Each can buffer a 9 MiB request and load up to a million ref versions (about 200 MB) to plan a clone | Size it by memory. Raise it while fetches queue and memory has room |
| `ASPEN_MAX_CONCURRENT_PUSHES` | `16` | `git-receive-pack` requests and SSH push sessions at once. Each reads its pack on a blocking thread (tokio has 512, shared with gzip decoding and local blob reads) and holds delta-base prefetch memory | The first setting to raise for push throughput: a small push spends most of its time waiting on the object store and the database, not on CPU. Raise it while pushes are refused with `503` and CPU, memory and the database have room, and keep it well below 512. More pushes at once also need more prefetch slots (`ASPEN_PREFETCH_MEMORY_BYTES` / `ASPEN_PREFETCH_FLOOR_BYTES`) and database connections |
| `ASPEN_MAX_CONCURRENT_STITCHES` | `32` | Inline packs being built and streamed at once. Each holds its plan's object IDs while it streams | Size it by memory, like fetches |
| `ASPEN_ADMISSION_QUEUE` | `64` | Requests of each kind that may wait for a slot; `0` refuses as soon as every slot is taken | Raise it to absorb bursts instead of answering `503` |
| `ASPEN_ADMISSION_QUEUE_WAIT_SECS` | `10` | Longest a request waits in that queue for a slot before it is refused; `0` refuses at once when no slot is free. At most `3600` | Keep it under your load balancer's idle timeout. Lower it so an overloaded instance sheds load sooner |
| `ASPEN_MEMORY_HIGH_WATER_BYTES` | 80% of the memory limit | Resident memory over which new fetch, push and stitch requests are refused at once with `503` and `Retry-After` (a retryable `ERR` over SSH), until it falls back under it. Work already running finishes. The default is 80% of the process's cgroup memory limit, or of the machine's memory without one; `0` turns it off | Keeps an overloaded instance answering instead of being killed by the kernel, which loses every request in flight. Set it a little under what the instance may use when other processes share its memory. Refusals count in `aspen_admission_memory_refusals_total` |

Request bodies are bounded too, for every caller: a fetch request is at
most 9 MiB (the protocol parser's 8 MiB plus framing), and any Git request
body fails once the client has sent nothing for 60 seconds, or, after its
first 10 seconds, once it has arrived slower than 1 KiB/s on average over
the time the server waited for it.

### Push path memory and CPU

A push that changes files needs the stored versions they were changed
from (its **delta bases**) to complete its pack. These are read from
stored packs into memory that all concurrent pushes share. A push first
reserves a small **floor** of it, and grows to its full per-push budget
only when its bases need more; while there is no room, it waits.

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_PREFETCH_MEMORY_BYTES` | `4294967296` | Memory for delta bases (4 GiB) shared by every push on the instance. It admits `ASPEN_PREFETCH_MEMORY_BYTES / ASPEN_PREFETCH_FLOOR_BYTES` pushes at once (64 by default) and `ASPEN_PREFETCH_MEMORY_BYTES / ASPEN_PREFETCH_PER_PUSH_BYTES` large ones (2) | Raise it when `stage="gate_wait"` grows and memory has room. `aspen_push_prefetch_slots` shows how many pushes it admits at once |
| `ASPEN_PREFETCH_FLOOR_BYTES` | `67108864` | What each push reserves (64 MiB) before its first read of stored packs. At most `ASPEN_PREFETCH_MEMORY_BYTES` and the per-push budget | Lower it to admit more small pushes at once with the same memory. Raise it when `aspen_push_prefetch_escalations_total` is a large share of pushes, since each escalation waits for the full budget |
| `ASPEN_PREFETCH_PER_PUSH_BYTES` | `2147483648` | The most delta-base memory one push may use: by default four times `ASPEN_MAX_OBJECT_BYTES` and at least 1 GiB (2 GiB with the default object limit). A push whose bases need more is refused with an `unpack` error that is not retried. At least `ASPEN_MAX_OBJECT_BYTES`, at most `ASPEN_PREFETCH_MEMORY_BYTES` | Lower it, with `ASPEN_MAX_OBJECT_BYTES`, when no push you accept is that large, so more large pushes fit at once |
| `ASPEN_PREFETCH_WAIT_SECS` | `60` | How long a push waits for prefetch memory before it is refused with a retryable error. At most `3600` | Lower it so a saturated instance fails pushes fast for clients to retry. Each give-up counts in `aspen_push_prefetch_wait_timeouts_total` |
| `ASPEN_PREFETCH_PARALLEL_READS` | `256` | Object store reads one push keeps in flight: the entries of its delta bases' chains, which it reads in one round, and its lookups in sampled pack indexes. A round with more reads waits for a second wave, one more round trip. From 1 to 1024 | Raise it when `stage="prefetch"` dominates on a high-latency store. Lower it if the store throttles (S3 answers `503 Slow Down`) |
| `ASPEN_IDX_CACHE_BYTES` | `1073741824` | Memory for pack indexes (`.idx`, 1 GiB), shared by every push, used to find objects in stored packs. An index is about 6% of its pack, and a whole one also keeps 12 bytes per object: offsets and a map of the pack's delta chains, so a chain is read in one round trip. When the indexes one push searches do not all fit, the ones it probes first are kept whole and the rest as samples (see `ASPEN_IDX_WHOLE_MAX_BYTES`), so a repository larger than the cache never reloads its own indexes; concurrent pushes that miss the same index share one read. It is a bound, not a reservation | The default holds a repository of about 30 million objects whole. Lower it on a small instance; raise it when one very large repository dominates and its lookups show `.idx` reads (a whole index answers without object store reads) |
| `ASPEN_OBJECT_CACHE_BYTES` | `67108864` | Memory for resolved delta bases (64 MiB), shared by every push. Consecutive pushes to one repository mostly need the same few bases (trunk's root tree and the directories above the files they change), and resolving one from a stored pack costs an object store read per delta in its chain. Objects over 1 MiB are never kept; `0` turns the cache off | Raise it for a repository with large trees and many pushes |
| `ASPEN_IDX_WHOLE_MAX_BYTES` | `67108864` | The largest pack index (64 MiB, a pack of about 2.4 million objects) kept whole in `ASPEN_IDX_CACHE_BYTES`. A larger one is kept as a sample, one object ID of every 256, and each lookup in it reads about 6 KiB of the index from the object store in one round trip; a whole index is read once, when the sample is built, with the pack's entry table, from which a map of its delta chains is written to `chains/` under the spool directory (16 bytes per object), so a chain in it is read in one round trip, not one per level. Without sampling, a repository whose indexes do not fit the cache read them whole on every push | Keep it at most a quarter of `ASPEN_IDX_CACHE_BYTES`. Raise both when one large repository dominates and memory allows: a whole index answers without object store reads |
| `ASPEN_TRUNK_TREE_CACHE_BYTES` | `67108864` | Memory for trunk's newest trees (64 MiB), shared by every push. A push diffs each pushed commit's tree against its parent's and descends only into the directories it changed, so connectivity and the "already stored" test look up only what changed; the parent's trees come from here (a push to trunk adds its own). `0` keeps none: each push reads the old trees on its changed paths from stored packs | Raise it when pushes to a repository with very large directories (thousands of entries) show `stage="graph"` time |
| `ASPEN_PUSH_THREADS` | every core | Threads one push uses to complete and index its pack. From 1 to 1024 | Many small pushes at once each need only one thread, and every core per push makes them compete: try `1` or `2` when `stage="complete"` grows under concurrency. Leave it unset for few, large pushes |
| `ASPEN_SYNC_COMMIT_ROWS` | `10000` | Commit index rows a push writes in its own database transaction. A push with more new commits writes its tip's row and leaves the rest to a background `index_commits` job. From 1 to 1000000 | Lower it so pushes that carry long histories are acknowledged sooner. Until the job runs, ancestry checks against those commits read packs, which is slower |

### Uploads to the object store

A push is acknowledged only after its pack, index and entry table are
durable in the object store. These settings apply to `s3://` stores (S3,
S3 Express One Zone, and S3-compatible stores); a `file://` store ignores
them.

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_UPLOAD_PART_BYTES` | `16777216` | Files up to this size (16 MiB) go in one `PUT`; larger ones in a multipart upload with parts of this size. From 5 MiB to 5 GiB, S3's limits. An upload has at most 10,000 parts (156 GiB with the default) | Most pushes are far smaller than a part and take one `PUT` either way. Raise it for fewer requests per large pack, at the memory cost below |
| `ASPEN_UPLOAD_CONCURRENCY` | `4` | Parts of one multipart upload in flight at once. Memory per upload is about part size × (concurrency + 2). From 1 to 64 | Raise it when large pushes or compaction spend their time in `stage="upload"` and the network has room |

### Metadata database

Requests take database connections in arrival order, so a busy pool slows
requests down but does not fail them (issue #150). A push holds a
connection for each short lookup and for its commit transaction. Every Git
request needs its repository's row and, when it carries one, its token's
row; each instance keeps both in memory for a few seconds, so a busy
caller's requests cost the database no lookup for them.

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_DB_MAX_CONNECTIONS` | `10` | Connections in the pool, per instance | Raise it while the database has CPU to spare and `stage="commit"` grows. Keep instances × connections under the database's `max_connections`; on PostgreSQL, 2 to 4 connections in total per database vCPU is a good start |
| `ASPEN_DB_ACQUIRE_TIMEOUT_SECS` | `30` | A request waiting for a connection fails with "internal storage error; retry" only after this many seconds pass with no connection returned to the pool (the database is unreachable, or every connection is stuck). A long queue alone never fails a request. `0` waits for ever; at most `31536000` (a year) | Rarely needs a change. Lower it to fail faster during a database outage |
| `ASPEN_DB_LOCK_TIMEOUT_SECS` | `10` | How long a statement waits for a row lock (PostgreSQL `lock_timeout`, SQLite `busy_timeout`) before the request fails with a retryable error. Two pushes racing for one branch wait this long at most for the winner. `0` is no limit; at most `86400` (a day) | Lower it so one stuck writer holds the others up for less time. Write transactions last milliseconds, so this firing means something is stuck |
| `ASPEN_DB_STATEMENT_TIMEOUT_SECS` | `60` | How long one statement may run on PostgreSQL before the database cancels it (`statement_timeout`); SQLite has no equivalent. Migrations run without it. `0` is no limit; at most `86400` (a day) | Every statement is a keyed lookup or a bounded batch. Lower it to cut runaway queries short |
| `ASPEN_GROUP_COMMIT` | `off` | `on` lets concurrent pushes share their metadata transactions: pushes that reach the database within `ASPEN_GROUP_COMMIT_WINDOW_MS` of each other register their packs in one transaction and commit their ref updates in another, instead of two transactions each. Each push is still answered only after its pack is in the object store and its own ref update has committed, and its compare-and-swap is its own (one that loses is retried alone). Each push still stores its own pack objects | Turn it on when `stage="commit"` grows under load, or the database's transaction rate or connection pool (`ASPEN_DB_MAX_CONNECTIONS`) is the limit. Watch `aspen_push_group_commit_pushes_total` / `aspen_push_group_commit_batches_total`: near 1 push per transaction, the window only adds latency |
| `ASPEN_GROUP_COMMIT_WINDOW_MS` | `2` | How long a group commit batch waits for more pushes after its first, from 0 to 1000 milliseconds; each push waits at most twice. `0` batches only pushes already waiting. Acts only with `ASPEN_GROUP_COMMIT=on` | Raise it (5 to 20) when batches stay small under load; each millisecond is added to every push's latency, up to twice |
| `ASPEN_GROUP_COMMIT_MAX_BATCH` | `64` | Most pushes one group commit transaction takes, from 1 to 1024; a full batch starts at once. Acts only with `ASPEN_GROUP_COMMIT=on` | Lower it if one transaction of many pushes holds row locks long enough to slow other writers; raise it only when batches reach it under load |
| `ASPEN_REPO_CACHE_TTL_MS` | `5000` | How long an instance keeps a repository's row in memory after reading it, in milliseconds, so a caller's requests do not each read it. A repository that does not exist is never remembered, so one created with `aspen repo create` is served at once. A setting changed in the database directly is seen within this long. From `0` (read on every request) to `60000` | Leave it. Lower it only if you edit `repos` rows by hand and need the change to apply sooner |
| `ASPEN_TOKEN_CACHE_TTL_MS` | `5000` | How long an instance keeps a token's row in memory after reading it, in milliseconds, so a caller's requests do not each read it. This is the longest a token revoked with `aspen token revoke` (or on another instance) keeps working; expiry still applies to the second. From `0` (read on every request) to `60000` | Lower it for faster revocation, at one token lookup per caller per instance per TTL. With it at `0` every Git request reads its token |

### Ref listings

Stock `git ls-remote <url> <ref>` asks for every ref (its patterns match the
end of a ref name, so it sends no `ref-prefix`), and agents that poll for a
ref run it often. Each instance keeps every repository's refs it has listed
in memory, and an `ls-refs` request asks the database only which refs
changed since the last listing: one statement, which reads no ref rows when
nothing moved, instead of a scan of the repository's refs plus a lookup of
`HEAD`. Concurrent requests for one repository share one such read.

A listing is never stale. Every request is answered from a database read
that began after the request arrived, so a push acknowledged by any
instance is in every listing requested after the acknowledgement, on every
instance.

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_LS_REFS_CACHE_REFS` | `250000` | Most refs this instance keeps in memory for `ls-refs` across repositories, about 160 bytes each; each repository counts one more. Least recently listed repositories are dropped first, and a repository with more live refs than this is listed from the database on every request, as with `0`. `0` turns the cache off; at most `100000000` | Raise it when repositories with many refs are polled and memory has room. A repository dropped from memory costs one full listing the next time it is requested |

### Background work

Each instance runs background workers: they build compaction covers, write
commit index rows for large pushes, and collect garbage. They never hold up
a push's acknowledgement, but while compaction falls behind, fetches and
pushes search more small packs.

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_COMPACT_WORKERS` | `8` | Background jobs (compaction and commit indexing) this instance runs at once, from 1 to 64. One of them also runs the garbage-collection passes ([below](#garbage-collection)) | A job mostly waits on the object store, so raise it while `aspen_jobs_queued_lag_seconds{kind="compact"}` grows and CPU has room |
| `ASPEN_COMPACT_IDLE_POLL_MS` | `5000` | How often an idle worker asks the database for a due job, in milliseconds. From 10 to 3600000 | Lower it so a new job starts sooner after a quiet spell. Every idle worker on every instance polls, so the database sees workers × instances / interval queries per second |
| `ASPEN_ROLE` | `all` | What this process runs: `all` (Git and the background worker), `serve` (Git, the merge queue and mirror mode, no background worker) or `compact` (the background worker only: compaction, commit indexing and garbage collection; `ASPEN_LISTEN` answers only `/healthz` and `/readyz`, and `ASPEN_SSH_LISTEN` or `ASPEN_MERGE_QUEUE=on` refuse the start). A deployment with `serve` processes needs at least one `compact` or `all` process | Run `compact` processes beside `serve` ones to scale compaction apart from Git, so it does not compete with pushes for CPU, disk and the database pool. Add `compact` processes (or `ASPEN_COMPACT_WORKERS`) while `aspen_compaction_backlog` grows under a steady push rate; flat means compaction keeps up |
| `ASPEN_MERGE_QUEUE_BATCH` | `64` | Most queued changes one trunk update of the merge queue (`ASPEN_MERGE_QUEUE`, under [push limits](#push-limits)) lands, from 1 to 1024 | Larger batches land more changes per trunk update, but a batch is rebuilt when trunk moves meanwhile |

### Garbage collection

Garbage collection deletes packs nothing names any more (a push that lost
its race, an evicted shallow pack) two hours after they were last
registered. It runs in processes whose `ASPEN_ROLE` runs the background
worker, one process at a time: a pass first takes a lease in the
database, and while another process holds it the pass does nothing. If
the holder stops, its lease expires and the next process to try takes it
over. The sweep reads registrations in the order their grace periods end,
resuming where it stopped, in transactions of a bounded size, so a pass
costs about the same with a thousand packs or a million. On PostgreSQL it
uses a connection pool of its own, so it never waits for, or holds, the
connections pushes use. `aspen_gc_*` on `/-/metrics` shows which process
collects and how long each batch takes ([Observability](reference/observability.md)).

| Variable | Default | Meaning | Tuning |
|---|---|---|---|
| `ASPEN_GC_EVERY_SECS` | `300` | Seconds between one process's garbage-collection passes, from 10 to 86400. Every process that runs the background worker tries this often; only the lease holder collects | Raise it to collect less often. Garbage waits at most about two hours plus twice this |
| `ASPEN_GC_SWEEP_BATCH` | `1000` | Registrations one sweep transaction reads, from 1 to 100000. It also reads as many packs of a slow audit, which walks every pack by ID to find packs with no registration time and any orphan the ordered walk passed. A pass runs transactions until the sweep has caught up, for at most 30 seconds | Lower it if `aspen_gc_sweep_batch_duration_seconds` is long enough to matter to the database. Raise it if a pass often stops at 30 seconds before catching up (`aspen_gc_sweep_rows_examined_total` grows by much more than new pushes) |
| `ASPEN_GC_LEASE_SECS` | `300` | How long the garbage-collection lease lasts after its holder last used it, from 10 to 86400. Every sweep transaction extends it; work under a lease that expired or changed hands is refused, so two processes never sweep at once | Keep it at least `ASPEN_GC_EVERY_SECS`, so the holder keeps the lease from one pass to the next. Lower both to hand over sooner when the holder stops |
| `ASPEN_GC_DB_MAX_CONNECTIONS` | `1` | Connections in garbage collection's own pool on PostgreSQL, apart from `ASPEN_DB_MAX_CONNECTIONS`, from 1 to 16. SQLite has one writer at a time, so there garbage collection shares the main pool | One is enough: a pass runs one statement at a time |
| `ASPEN_GC_DB_STATEMENT_TIMEOUT_SECS` | `10` | How long one garbage-collection statement may run on PostgreSQL before the database cancels it (`statement_timeout`), apart from `ASPEN_DB_STATEMENT_TIMEOUT_SECS`. A cancelled pass is retried at the next interval. `0` is no limit; at most `86400` | Each statement is a bounded range scan that takes milliseconds; this firing means the database is overloaded or a plan went wrong |

## Push limits

These bound what one push may cost the server, whatever the caller. The
defaults suit a large monorepo.

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_MAX_OBJECT_BYTES` | `536870912` (512 MiB) | Largest single object, inflated |
| `ASPEN_MAX_INFLATED_BYTES` | `8589934592` (8 GiB) | Most one push may inflate to in total; at least `ASPEN_MAX_OBJECT_BYTES` |
| `ASPEN_MAX_PUSH_BYTES` | `8589934592` (8 GiB) | Largest pack per push |
| `ASPEN_MAX_PUSH_OBJECTS` | `10000000` | Most objects per push (checked against the pack header before the body is read) |
| `ASPEN_MERGE_QUEUE` | `off` | `on` runs the merge queue: push a change with `git push origin HEAD:refs/queue/main/<your-name>` and the server lands it on `main` in a batch with other pending changes, one compare-and-swap per batch. Or push a branch with `git push -o land origin HEAD:refs/heads/<branch>`: the branch is pushed and also queued as `refs/queue/main/<branch>`, in the same transaction. Read the outcome with `git ls-remote origin refs/queue-done/main/<your-name> refs/queue-rejected/main/<your-name>`: the first names the trunk commit that landed it; the second a report commit (`git fetch origin refs/queue-rejected/main/<your-name> && git log -1 FETCH_HEAD`) listing the paths that conflicted. A change of several commits lands as one commit with the last one's message |
| `ASPEN_MERGE_QUEUE_OUTCOME_TTL_SECS` | `3600` | Seconds an outcome ref (`refs/queue-done/...`, `refs/queue-rejected/...`) is kept after it was written; then the queue deletes it (a compare-and-swap delete, so a reused name keeps its newer outcome). `0` keeps outcomes for ever. Every outcome is a ref, so without expiry a busy repository gains thousands an hour and every full ref listing grows |
| `ASPEN_MERGE_QUEUE_INTERVAL_MS` | `50` | How long a merge-queue batch that is not full waits, after its oldest change was queued, for more changes to land with it (0 to 10000; `0` lands as soon as anything is queued). Longer means fewer trunk updates and uploads per change, at the cost of latency. One worker per repository builds batches at a time, under a lease other instances take over within 10 s if it stops |

Memory: plan for about 140 bytes of heap per object of the largest push you
accept, plus the prefetch memory ([push path memory](#push-path-memory-and-cpu)).
The database pool, the prefetch memory, the background workers and the
merge queue's batch size are under [throughput tuning](#throughput-tuning).

## Fetch delivery

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_PACKFILE_URIS` | `auto` | Hand out packfile URIs to clients that ask (`fetch.uriprotocols`). `auto` turns them on when the store can presign URLs or express routes are configured; `true` or `false` to force |
| `ASPEN_SHALLOW_PACKS` | `true` | Serve `--depth=N` fetches (N up to 64) exact shallow packs, built once per branch version and depth. `false` sends every shallow fetch the full history of its wants |
| `ASPEN_SHALLOW_MAX_REPO_BYTES` | `8589934592` (8 GiB) | The most one repository's exact shallow packs may take in the bucket. A new one evicts the least recently used first; one larger than this on its own is not stored, and its fetch gets the full history |
| `ASPEN_SHALLOW_UNUSED_SECS` | `86400` (24 hours) | How long an exact shallow pack nobody fetches is kept, for current branch tips too; at least `600` |
| `ASPEN_EXPRESS_KEYS` | unset | `id:hexsecret[,id:hexsecret...]`. Signs (first key) and verifies (every key) express and CDN URLs. The same on servers and express nodes; a secret |
| `ASPEN_EXPRESS_URLS` | unset | Comma-separated express nodes clients can reach, such as `https://git.example.com/-/express` |
| `ASPEN_EXPRESS_SERVE` | `auto` | Serve `/-/express` from this server. `auto` does when `ASPEN_EXPRESS_URLS` names `<ASPEN_PUBLIC_URL>/-/express`; `true` for a CDN whose origin is this server |
| `ASPEN_EXPRESS_TTL_SECS`, `ASPEN_EXPRESS_WINDOW_SECS` | `600`, `300` | Shortest life of a signed URL, and the window expiries round up to (together at most an hour) |
| `ASPEN_CDN_URL` | unset | Base URL of a CDN whose origin is express; tried before `ASPEN_EXPRESS_URLS` |
| `ASPEN_CDN_PAST_END` | `416` | What that CDN answers for a range past the end of a pack: `416` (URLs go only to Git 2.56 and later) or `206` (every client) |

The AWS Terraform module sets all of these itself: every task serves
`/-/express`, CloudFront in front of it is the CDN tier, and the module
generates `ASPEN_EXPRESS_KEYS` into Secrets Manager.
[Express and CDN mode](reference/express.md) explains tiers, key rotation
and caching.

## Express nodes

`aspen express` runs a standalone pack cache for a region. It reads
`ASPEN_BLOB_URL` and `ASPEN_EXPRESS_KEYS` (the same values as the servers)
and:

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_EXPRESS_LISTEN` | `0.0.0.0:7071` | Listen address (also `--listen`) |
| `ASPEN_EXPRESS_CACHE_DIR` | unset (no cache) | Disk cache directory |
| `ASPEN_EXPRESS_CACHE_BYTES` | `10737418240` (10 GiB) | Disk cache budget, packs being filled included |
| `ASPEN_EXPRESS_CACHE_FILLS` | `4` | Cache fills that may run at once (at least 1); a miss past it is served from the blob store without filling |

## Git over SSH

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_SSH_LISTEN` | off | Address for Git over SSH, such as `0.0.0.0:2222` |
| `ASPEN_SSH_HOST_KEY_FILE` or `ASPEN_SSH_HOST_KEY` | required with SSH | The host key: an unencrypted OpenSSH Ed25519 or ECDSA private key, as a file or inline from a secret store. Clients pin it, so every instance must use the same key across restarts. Aspen never generates one |

Create a host key with `ssh-keygen -t ed25519 -N '' -f host_key`. More in
[Git over SSH](reference/ssh.md).

Secret files (`ASPEN_SSH_HOST_KEY_FILE` and the mirror credentials'
`*_FILE` variables) must be regular files that other users cannot access:
Aspen refuses to start with one that is not, and warns about one a group other than its own may
read. Use `chmod 600`, or `640` with Aspen's group.

## Mirror mode

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_MIRROR_CREDENTIALS` | unset | Comma-separated names of the credentials mirrors may use, such as `github,gitlab`. Each must be complete at startup |
| `ASPEN_MIRROR_<NAME>_TOKEN` or `ASPEN_MIRROR_<NAME>_TOKEN_FILE` | | An access token for credential `<NAME>` (upper-cased, `-` as `_`) |
| `ASPEN_MIRROR_<NAME>_USERNAME` | per forge | The HTTP user name for the token: `x-access-token` on github.com, `oauth2` on gitlab.com, `git` elsewhere |
| `ASPEN_MIRROR_<NAME>_GITHUB_APP_ID`, `ASPEN_MIRROR_<NAME>_GITHUB_INSTALLATION_ID` | | A GitHub App instead of a token |
| `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY_FILE` or `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY` | | The app's private key (PEM) |
| `ASPEN_MIRROR_<NAME>_GITHUB_API_URL` | `https://api.github.com` | The REST API, for GitHub Enterprise Server |
| `ASPEN_MIRROR_SYNC_SECS` | `60` | Seconds between sync passes of each mirror |
| `ASPEN_MIRROR_WORK_DIR` | `<ASPEN_SPOOL_DIR>/mirror` | Spool for packs to and from upstreams; a real disk |
| `ASPEN_MIRROR_MAX_PACK_BYTES` | `8589934592` (8 GiB) | Largest pack fetched from an upstream |

The one-off commands `aspen import` and `aspen mirror ...` read
`DATABASE_URL`, `ASPEN_BLOB_URL` and the credentials above, and spool in
`ASPEN_WORK_DIR` (default: a directory under the system temporary
directory; also `--work-dir`). See [Mirror mode](reference/mirror.md).

## Logs, metrics and traces

| Variable | Default | Meaning |
|---|---|---|
| `ASPEN_METRICS_LISTEN` | `127.0.0.1:9464` | Private listener for Prometheus metrics at `/-/metrics`, or `off`. Must differ from `ASPEN_LISTEN`; keep it off every public load balancer |
| `ASPEN_LOG` | `info` | What is logged, in `tracing` filter syntax (`info,aspen_server=debug`) |
| `ASPEN_LOG_FORMAT` | `text` | `text`, or `json` for one JSON object per line |
| `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | unset (no traces) | Export traces over OTLP/HTTP to this collector. The first gets `/v1/traces` appended; the second is used as is |
| `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `http/protobuf` | Only `http/protobuf` is supported; anything else is refused at startup |
| `OTEL_SERVICE_NAME` | `aspen` | The traces' `service.name` |
| `OTEL_SDK_DISABLED`, `OTEL_TRACES_EXPORTER` | unset | `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` turns export off |

The OpenTelemetry exporter also honours `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_EXPORTER_OTLP_TIMEOUT` and `OTEL_RESOURCE_ATTRIBUTES`. Metric names,
the dashboard and the alert rules are in [Observability](reference/observability.md).

## Endpoints

| Path | Listener | What |
|---|---|---|
| `/<repo>.git/...` | Git | Git smart HTTP (push needs `write`, fetch `read`) |
| `/healthz` | Git | `200` while the process serves HTTP. Use for liveness |
| `/readyz` | Git | `200` once the database answers, `503` otherwise. Use for load balancer readiness |
| `/-/blobs/...` | Git | Signed pack downloads from a `file://` store |
| `/-/express/...` | Git, when served | Signed pack downloads through express |
| `/-/metrics` | metrics | Prometheus metrics |

`aspen healthcheck [--url http://127.0.0.1:7070/readyz]` exits 0 only on a
`200`; the container image has no curl, so container health checks use it.
