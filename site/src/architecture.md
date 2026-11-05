# Architecture

This page explains how Aspen stores repositories and serves pushes and
fetches. The full design, with every rule and the reason for it, is the
[design plan and decisions log](reference/plan.md); the database tables are
described column by column in the [metadata schema](reference/schema.md).

```text
git client ─HTTP/SSH─► aspen serve ──compare-and-swap per branch──► PostgreSQL
                            │                                       (refs, versions, commits, covers, jobs)
                            │ stream, check, complete
                            ▼
                     object store (S3 / GCS / R2 / MinIO / local disk) ◄── compaction worker
                            ▲
git client ◄─packfile URI── aspen express (regional pack cache) / CDN
```

Everything runs in one binary. `aspen serve` handles Git over HTTP and SSH,
runs the compaction worker and the mirror sync passes, and can serve express
downloads. Several instances can run against the same database and bucket
with no coordination between them: every write is a compare-and-swap or an
idempotent step.

## What is stored where

**Object storage holds packs, never loose objects.** Every pack is stored
with its `.idx` and an *entry table* (each entry's offset, type and delta
base), under `repos/<repo id>/packs/<checksum>.pack` and so on. Every stored
pack keeps two invariants:

- **Self-contained:** every delta's base is in the same pack. Git clients
  usually push *thin* packs, whose deltas refer to objects the server
  already has; Aspen completes them at ingest by inserting those bases.
- **Base-first:** each delta's base comes before the delta.

Together these let any stored pack be sent on its own (as a URL the client
downloads), or joined with others into one response in a single streaming
pass.

**PostgreSQL (or SQLite) holds the history of every branch.** The main
tables:

| Table | Holds |
|---|---|
| `refs` | Each branch and tag's current version. The only row a push contends on |
| `ref_versions` | Every version of every ref: its tip, the pack the push stored, and the versions it depends on (the trunk version it was built against, and for a branch-scoped push, the branch's previous version) |
| `packs` | Every stored pack: checksum, size, object count, level |
| `commit_index` | Commit → a version that reaches it, its parents and its time: resolves fetch requests and answers "is A an ancestor of B" without reading packs |
| `covers` | Compacted ranges of a ref's versions and the pack that holds them |
| `jobs` | The background queue: indexing, compaction, mirror pushes, pack repairs |

A ref version's **closure** is every object it needs: the objects in its
own pack plus the closures of the versions it depends on. Every committed
version is *connected*: every object its tip reaches is in its closure.

## Pushing

1. **Advertise.** The client is shown trunk only, plus up to 8 earlier
   trunk tips as `.have` lines so a client that is behind does not resend
   history. A [branch-scoped URL](using-git.md#urls) also shows that
   branch.
2. **Receive.** The pack streams to a local spool file while it is parsed:
   every entry is inflated and checksummed, with SHA-1 collision detection.
   Size and object-count limits apply before the body is read.
3. **Complete.** Delta bases the pack refers to but does not contain are
   looked up in the packs of the versions the client was shown (trunk, and
   the scoped branch), fetched in parallel, and inserted. The completed pack
   is re-hashed and indexed. Checks reject an object stored twice and
   repair a delta that comes before its base.
4. **Check connectivity.** Every object the pack references but does not
   contain must exist in the push's dependencies.
5. **Decide.** Each ref update gets a verdict: create, fast-forward (Aspen
   checks ancestry itself, since the client never saw the branch), forced
   (only with `-o force` or a lease), delete, or no-op.
6. **Upload.** The pack, its `.idx` and entry table go to object storage.
   A push to several refs may store more than one pack, so that each
   branch's version stays exact (below).
7. **Commit.** One database transaction inserts the new ref versions,
   moves each ref by compare-and-swap, writes commit rows and enqueues
   jobs. If a concurrent push moved the ref first, Aspen recomputes the
   verdict against the new version, or answers with a retryable error. It
   never commits a verdict computed against a stale version.

Pushes to different branches share nothing but the trunk version they were
built against, which is only read. Two pushes to the same branch race on
one row, and one of them loses cleanly.

## Fetching

1. **List refs.** Protocol v2's `ls-refs` honours `ref-prefix`, so a client
   fetching one branch does not receive a list of every ref.
2. **Plan.** Each *want* (a commit the client asks for) resolves to a ref
   version through `commit_index`. Each *have* (a commit the client already
   has) stands for a version only when Aspen can prove the client holds
   that version's whole closure; other haves are ignored, which costs bytes
   but never correctness. The planner walks the dependency graph and picks
   the packs the client lacks, using compacted covers to keep the pack count
   small.
3. **Deliver.** Clients that opted in (`fetch.uriprotocols https`) get an
   empty inline pack and one signed URL per planned pack, through a CDN, an
   express cache or the bucket. Everyone else gets a **stitched** pack: the
   planned packs' entries streamed in order, with each object sent once,
   delta offsets rewritten, and nothing decompressed or recompressed.
4. **Shallow fetches** get an exact `shallow-info` computed from
   `commit_index`. A plain `--depth=N` fetch of a branch tip is served an
   exact shallow pack, built once per version and depth and stored like any
   other pack; other shallow forms get the full history of their wants.

### Exact and wider versions

A have can stand for a version only if the version's closure is exactly
what its tip reaches. Aspen records this per version. A version is *exact*
when its pack holds only objects its tip reaches and every version it
depends on is exact with a tip that is an ancestor of its own. Wider
versions are correct but make incremental fetches send more. Aspen keeps
trunk exact in normal use: a push to several refs splits its objects so
that trunk's pack holds only what trunk reaches, and a forced trunk update
picks a dependency that keeps it exact. A metric and an alert report a
repository whose trunk became wider ([Operations](operations.md)).

## Compaction

Every push stores a small pack. In the background, the compaction worker
merges them per ref in levels: every 16 versions into a level-1 pack, every
16 level-1 packs into a level-2 pack, and so on up to level 4. Merged packs
are written base-first and self-contained, with delta chains capped at 50,
and are verified before they are uploaded. Original packs are kept, since
their versions still name them.

Covers serve two purposes. A full clone sends a handful of large packs
instead of thousands of small ones. And when a cover's range is a
*dependency chain* (every version depends on the one before it), a fetch
from a client that is current on trunk can skip through the whole range at
once. In the planner's tests, fetching a branch created 1,000 or 5,000
trunk pushes ago visits 33 to 43 versions instead of every one of them.

Packs that no version, cover or job names (a push that lost its race, for
example) are deleted by garbage collection after a grace period of 24
hours. A pack registered again after its earlier copy was collected gets a
new *generation* in its object keys, so a late deletion can never remove
the new copy.

## Crates

The code is a Cargo workspace. Dependencies point downward; CI enforces the
rules (`cargo xtask check-deps`).

| Crate | Role |
|---|---|
| `aspen-core` | Types, traits and the rules every crate shares (the commit check, closures, job states, errors); depends on no other Aspen crate |
| `aspen-proto` | Git wire protocol: pkt-line, receive-pack, upload-pack v0/v1/v2, side-band. No I/O |
| `aspen-pack` | Pack ingest and completion, `.idx` and entry tables, the stitcher, repacking |
| `aspen-planner` | Fetch planning; pure functions, property-tested against `git rev-list` |
| `aspen-meta` | The PostgreSQL and SQLite store and its migrations |
| `aspen-blob` | Object storage (S3-compatible and local), presigned URLs |
| `aspen-compact` | Compaction, commit indexing, exact shallow packs, garbage collection |
| `aspen-express` | The signed-URL pack cache |
| `aspen-mirror` | Mirror mode: a Git client of the upstream |
| `aspen-auth` | Tokens, OIDC, SSH keys, grants, rate limits |
| `aspen-server` | HTTP and SSH services, wiring, metrics |
| `aspen-cli` | The `aspen` binary |

Only `aspen-server` and `aspen-cli` pick concrete stores; every other crate
works through the traits in `aspen-core`.

## How it is tested

What runs today:

- **End-to-end tests with stock Git** in required CI: pushes, clones,
  fetches, shallow fetches, races, SSH, mirror mode and the `aspen` binary,
  checked with `git fsck --strict`.
- **Planner property tests** check plans against `git rev-list --objects`
  on real repositories (64 seeds in required CI, 2,000 nightly).
- **Protocol tests** replay recorded libgit2, go-git and Git sessions byte
  for byte in required CI, and run libgit2 and go-git live nightly.

Built but not yet pointed at Aspen:

- A **differential oracle** that runs random sequences of pushes, forces,
  deletes and concurrent pushes against two servers and compares refs and
  objects after every step. It runs nightly against stock `git
  http-backend`.
- A **client matrix** (Git 2.30 to latest, JGit, libgit2, go-git,
  gitoxide), also run nightly against stock Git.

Chaos testing and fuzzing are being added.

The launch gate (section 7 of the [plan](reference/plan.md#7-launch-gate-all-true-before-the-demo-is-public))
lists what has to be true before Aspen is called ready.
