# aspen-chaos

Aspen's chaos suite (node D1 in [`docs/PLAN.md`](../../docs/PLAN.md),
section 4, "Chaos testing"). It runs real `aspen serve` processes, sends
them load from stock `git` clients, breaks things underneath them, and
checks that three promises hold:

1. **An acknowledged push is never lost.** Every commit whose push got an
   `ok` from the server is still in the history of its ref.
2. **No ref ever points at objects that can't be fetched.** Every ref can
   be fetched (full, incremental, with packfile URIs, and `--depth=1`) and
   the result passes `git fsck --strict`.
3. **History per ref is linearizable.** The reads and pushes clients saw
   can be put in one order, consistent with real time, that a
   compare-and-swap register would produce.

The suite is black-box. It talks to Aspen only through Git over HTTP and
reads the alert counters from `/-/metrics`. It uses `psql` to read the job
queue, and to end job leases (`UPDATE jobs`): those of an instance it
killed, and, if the queue has not drained 90 s into the final checks,
those live workers still hold (see [Compaction](#scenarios)). It depends
on no product crate, only on `aspen-testkit`.

A run also fails when it could not check what it claims, so a green run
cannot be an empty one (see [Guards](#guards)).

## What runs

```text
 git clients ──► front proxy ──► aspen serve × N ──► db proxy ──► Postgres primary
 (6 threads,     (round robin,     (the real binary,  (partitions,    │ synchronous
  stock git)      partitions,       one spool each,    closes,        ▼ replication
                  closes, delay)    compaction worker) latency)    Postgres standby
                                         │
                                         └──► S3 proxy ──► MinIO (CI) or the local
                                              (errors, closes     file store
                                               before/after a
                                               write, truncation,
                                               latency, stalls)
```

- **Instances.** Several `aspen serve` processes share one database and
  one bucket, as replicas behind a load balancer do. The front proxy
  spreads connections over the live ones, so a push's two HTTP requests
  can land on different processes, and concurrent pushes meet in the
  database's compare-and-swap rather than in one process.
- **Proxies.** The faults come from proxies in this crate, not from
  Docker or toxiproxy: a TCP proxy for Postgres and for clients, and an
  HTTP/1.1 proxy for S3. The S3 proxy understands requests, so it can do
  what a TCP proxy cannot: let an upload land and then drop the answer.
- **Postgres.** Started from the machine's binaries (`initdb`, `pg_ctl`,
  `pg_basebackup`, `psql`). For failover the primary has a synchronous
  standby, like a managed database with a standby in another zone. A
  failover with asynchronous replication can lose acknowledged commits by
  design; that is a deployment choice, not something Aspen can fix.
- **Clean environment.** Aspen processes get a cleared environment. The
  S3 endpoint must be on loopback. Nothing reaches a real cloud account.

### The workload

Each client loops over:

- **push** to trunk (`main`, through the repository URL), to a shared
  branch every client pushes to, or to its own branch. It fetches the ref
  first (that fetch is a read of the tip `P`), builds one to three commits
  on `P` with `git fast-import`, and pushes the last one. Branch pushes go
  through the branch URL (`<repo>.git/-/<ref>`) or, half the time, through
  the repository URL, where Aspen advertises only trunk: the client then
  sends a zero old value and Aspen's own fast-forward check decides;
- **read** a tip with `git ls-remote`;
- **fetch** with `--depth=1`, or with `fetch.uriprotocols` (packfile URIs).

Pushes only fast-forward and every commit is unique, so a push of `C` on
`P` succeeds exactly when the ref is still at `P`: a compare-and-swap.
Every fetch runs with `fetch.fsckObjects`. Each operation is recorded with
its start and end on one monotonic clock, and with its outcome:

| Outcome | When |
|---|---|
| ok | The server's `ok` line for the ref, or a read's answer |
| fail | The ref was not at the push's base: `fetch first`, `non-fast-forward`, `incorrect old value`, a lost compare-and-swap (`failed to update ref`, stock Git's `cannot lock ref ... but expected`), `stale info` |
| no effect | Refused for a reason that says nothing about the ref: `cannot decide ancestry yet`, a lock the server could not take. The checker ignores it |
| unknown | Anything else: a broken connection, a timeout, `internal storage error; retry` (which may come after the write). It may take effect any time after it started, or never |

A server-side rejection (`[remote rejected]`) counts as definite only when
the push sent its request once. libcurl resends a request whose reused
connection dies before an answer, so one `git push` can deliver its
`git-receive-pack` POST twice: the first copy lands and the second is
refused (#132).
Every push runs with `GIT_TRACE_CURL`; a server-side rejection after two
POSTs is unknown and counted as "push rejected after a resend". A
rejection the client made itself from the advertisement (`[rejected]`)
stays definite.

### Packfile URIs

Git falls back to an ordinary pack without a word when a server sends no
URIs, so the suite counts them: fetches that ask for URIs run with
`GIT_TRACE_PACKET`, and the report shows "packfile URIs used". A run of
`aspen-chaos` fails if that count is zero. Git before 2.56 accepts only
URLs that Aspen serves itself (spike S3), so with S3 every instance also
serves express URLs (`ASPEN_EXPRESS_URLS` through the front proxy, no disk
cache, so downloads read through the faulted S3 proxy); with the local file
store, Aspen's own `/-/blobs` URLs are used.

### Epochs

A run is a series of epochs:

1. **Active** (30 s by default): clients work; the fault injector takes a
   step every 0.3 to 2.5 s. At the end it heals everything.
2. **Quiet** (3 s): nothing runs, so work a fault left in flight on the
   server can finish.
3. **Checks**: read every tip; check each ref's epoch history for
   linearizability; check every acknowledged push since the repository was
   created against its ref's tip; fetch every ref into a fresh repository
   (and, every other epoch, again with packfile URIs), into a persistent
   observer repository (incremental), and trunk with `--depth=1`; run
   `git fsck --strict` on each. Durability walks the exact tip the
   boundary read returned (`git rev-list <oid>` in the observer).

A push with an unknown outcome can still land after its epoch (the server
kept going after the client gave up). It is carried into the next epoch's
history while it can still apply, that is while its ref is still at the
push's base. Commits never repeat, so once the ref moves on, the push can
never land.

After the last epoch, the final checks need the job queue to drain, report
dead jobs as warnings, and fail if any `aspen_alert_*` counter moved on any
instance (each instance is read before it is killed). The counters
`aspen serve` always exports (`aspen_alert_stored_pack_forward_base_total`,
`aspen_alert_store_corruption_total`, and with express
`aspen_alert_express_corrupt_pack_total`) must be present in every read. The queue gets 90 s;
then the suite ends the leases live workers still hold and gives it 90 s
more. A job that needed the second step was dropped by its worker without
an outcome (a warning; since
#131 (compaction) and
#188 (mirror mode) workers retry
recording an outcome and give back a claim whose answer was lost, so this
should only follow a store outage longer than the lease); one that still
does not finish is a violation.

### Scenarios

```console
$ aspen-chaos scenarios
calm        no faults; every check must pass on a healthy deployment
kill        SIGKILL random instances (sometimes all) during pushes; restart them
compaction  SIGKILL the instance holding a compact or index_commits lease, mid-job
s3          S3 503/500 answers, closes before and after a write lands, truncated reads, latency, partitions
db          database partitions, connection closes, latency, and Postgres crash-restarts
failover    crash the Postgres primary and promote its synchronous standby
net         client-side partitions, connection closes, refusals and latency
mixed       every fault above, interleaved at random
smoke       kill, failover, S3, database and client network faults once each, in turn
```

`s3`, `mixed` and `smoke` need an S3 store; with the local file store they
refuse to start. In `smoke` each fault takes its turn and keeps it until it
has injected once; in `mixed` half the steps go to a fault that has not
injected yet. In both, an epoch's active phase goes on past `--epoch` while
a fault has not injected yet, for at most 5 minutes from its start: a
Postgres failover waits for a new synchronous standby, so two of them can
use up a 30-second epoch, and the nightly `mixed` run of 2 epochs once
ended without a kill or a compaction kill. `smoke` leaves out the compaction kill: workers poll every
5 s and a job takes milliseconds, so a 12-second run cannot count on
catching one leased (on GitHub's runner it did not). `compaction` and
`mixed` include it, and fail if it never fires.

`compaction` watches the job table and kills exactly the process that
holds a `compact` or `index_commits` lease. Job leases last 10 minutes, so
after every kill the suite ends the dead process's leases in the database
(`lease_expires_us = 0` for that process only), which is what the lease
clock would do 10 minutes later. The suite therefore tests what happens
when another worker reclaims the job, not the lease timer itself (A3's
store tests cover that).

## Running it

You need `git`, the PostgreSQL binaries and a built `aspen`:

```bash
cargo build -p aspen-cli -p aspen-chaos
# Postgres: the runner's /usr/lib/postgresql/<n>/bin is found by itself;
# elsewhere point ASPEN_TEST_PG_BIN at a directory with initdb, pg_ctl,
# pg_basebackup and psql.
# S3 (needed for the s3 scenario; otherwise the local file store is used):
eval "$(crates/aspen-blob/scripts/test-minio.sh)"

target/debug/aspen-chaos run --scenario mixed --epochs 3
target/debug/aspen-chaos run --scenario all --epochs 2 --epoch 30s
target/debug/aspen-chaos run --scenario mixed --duration 24h --out /var/tmp/chaos
target/debug/aspen-chaos run --help
```

Put `--out` on a real disk for long runs: where `/tmp` is memory (tmpfs),
the default (a directory under `$TMPDIR`) fills RAM. A scenario of two
30-second epochs uses about 130 MB there. The server's own data (packs in
the bucket, rows in Postgres) grows with every push and is not cleaned up
during a run; the local clones are, each time a fresh repository starts
(`--repo-epochs`, default 20).

The `aspen` binary is found through `ASPEN_BIN`; under cargo (the tests),
cargo builds it first so a test never runs a stale binary; else next to
`aspen-chaos` (`target/<profile>/aspen`). Exit status: 0 when every
invariant held, 1 when one broke, 2 when the suite could not run.

The run directory (`--out`, or a kept temporary directory) holds each
instance's log (`deployment/aspen-<n>.log`, with a line at every start and
kill), the whole operation history (`run/history.txt`) and, for a
linearizability failure, that ref's epoch history
(`run/violation-epoch<e>-<ref>.txt`).

### Where it runs

| Where | What | Time |
|---|---|---|
| Required CI (`test` job) | `tests/suite`: the checks against known servers, the guards, and `smoke` against Aspen (2 instances, standby, MinIO, 2 epochs of at least 6 s, each lasting until 10 pushes are acknowledged, at most 60 s), in parallel | about a minute |
| Nightly (`chaos` job) | `--scenario all --epochs 2 --epoch 30s`, 3 instances, 6 clients | about 20 minutes |
| On demand (`chaos-soak.yml`) | one scenario (not `all`, which would run the duration once per scenario) for up to 330 minutes | as asked |
| Launch gate | `--scenario mixed --duration 24h` on a dedicated host; GitHub-hosted jobs stop at 6 hours | 24 hours |

## Guards

A run fails, rather than passing, when it could not check what it claims:

| Finding | When |
|---|---|
| linearizability search ran out of budget | The search for a ref passed `--lin-budget` states (2,000,000 by default) |
| the run did not exercise what it claims | An epoch with no acknowledged push; a faulted scenario that injected no fault, or a part of `mixed`/`smoke` that never did; an epoch that reached its `active_until` limit before its condition held; no packfile URI used |
| a check or fault step could not run | A fault step returned an error (S3 faults without S3, an instance that would not restart); a scratch repository for a check could not be made; the durability walk failed; a client failed on its own repository three times in a row |
| instance exited without being killed | An `aspen serve` process exited on its own; it is recorded before it is restarted |
| alert counter moved or could not be read | Besides a moved counter: `/-/metrics` of a running instance did not answer 200, a value did not parse, or a counter above was missing |
| unavailable after faults were healed | Also: the job queue could not be read in the final checks |

`tests/suite/guards.rs` triggers each of them on purpose (a budget of one
state, a reads-only workload, an injector that never injects and one that
fails, an epoch waiting for a condition that never holds, metrics on a port nobody scrapes plus a SIGKILL from outside the
suite) and checks that the run fails.

## Reading a failure

The report names each broken invariant, the epoch, and the evidence:

```text
chaos scenario last-writer-wins: FAILED after 2 epoch(s), 12 s (seed 0x00000000000000d1)
  - [epoch 0] ref history not linearizable: refs/heads/main: no valid order: the best one
    places 6 of 39 completed operations, leaving the ref at cc674b694de7, and then none of
    these can come next:
        #7 [0.684s, 0.840s] client 1 refs/heads/main push 1b37615784bb -> 20ca8f68c388: Ok
           (2 commit(s); 1b37615..20ca8f6): acknowledged, but needs the ref at 1b37615784bb
        #5 [0.697s, 0.797s] client 3 refs/heads/main push 1b37615784bb -> 4588a97f8dc3: Ok
           (2 commit(s); 1b37615..4588a97): acknowledged, but needs the ref at 1b37615784bb
  - [epoch 0] acknowledged push lost: 6 acknowledged push(es) to refs/heads/main are not in
    the history of its tip 1df465fe76ce: ...
```

(That is from the suite's own test against a server that skips the
compare-and-swap: two overlapping pushes from the same base were both
acknowledged.) Each operation shows its id, its start and end in seconds
since the run began, the client, the ref, what it did and the server's
words. Find the same time in the instance logs. The seed replays the fault
schedule and the workload's choices; timing differs between runs, so a
failure may need several runs to come back.

## How we know the checks work

- `src/lin.rs` tests the linearizability checker on hand-written histories
  (a lost update, a stale read, an unexplained rejection, an unknown push
  that lands late or never) and on 200 random histories of a correct
  register under concurrency.
- `tests/suite/checks_catch_bugs.rs` runs the whole workload and every check
  against stock `git http-backend`, which must pass, and against testkit's
  broken servers, each of which must fail the invariant it breaks:
  last-writer-wins (not linearizable, acknowledged pushes lost), a dropped
  ref update (acknowledged push lost), a lost object (unfetchable). Each
  broken server records when its fault acts (`FaultyServer::fired`). Its
  run lasts until the fault has fired, not a fixed time
  (`RunConfig::active_until`), so a fault armed for the 10th push fires however busy the
  machine is, and each test checks that the fault fired before it checks
  that the fault was caught (#163).
- The `smoke` run against Aspen waits for its work the same way: each
  epoch lasts until Aspen has acknowledged 10 pushes
  (`ActiveUntil::acked_pushes`), so a slow runner gets a longer epoch
  instead of a failed run, and an epoch still short of them after 60 s
  fails the run as vacuous (#197).
- `tests/suite/guards.rs` shows that a run which could not check what it
  claims fails (see [Guards](#guards)).
- Against Aspen itself, a one-line mutation of the receive path's ancestry
  walk (answering "descendant" where it should say "not a descendant") made
  a 12-second `calm` run fail all three promises: 1 acknowledged push lost,
  1 non-linearizable history and 65 fetches with missing objects. The
  mutation is not in the tree; the PR that added the suite records the run.

## Findings so far

- #131: a database
  error while the compaction worker records a job's outcome leaves the job
  `running` for its whole 10-minute lease (seen after a failover).
- #132: a push whose
  POST libcurl resent after its connection was closed is reported rejected
  although its first copy landed.

## Not covered yet

- SQLite metadata (`aspen dev`): every scenario runs on Postgres.
- Git over SSH, mirror mode, force pushes, deletes and tags: the workload
  only fast-forwards over HTTP, which keeps the compare-and-swap model
  exact. The differential oracle (`testkit`) covers those semantics without
  faults.
- Garbage collection's sweep in runs shorter than its grace period (2
  hours since issue #203): required CI and the nightly scenarios never
  reach it; a soak of more than about 2 hours, and the 24-hour run, do.
- Disk faults (a full or failing spool), clock skew between workers, and
  kernel-level partitions (the proxies emulate them; nothing uses
  `iptables`).
