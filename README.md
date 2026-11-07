# Aspen

![Aspen: Git for a thousand agents at once.](docs/assets/aspen-banner.png)

Aspen is a Git server you run yourself, built for repositories that hundreds
of agents, CI jobs and people push to and fetch from at the same time. It
speaks plain Git over HTTP and SSH, so stock `git` works with no client
changes. It stores packs in object storage, keeps branch history in
PostgreSQL, and runs on one machine or on any cloud.

> **Status: pre-alpha.** Aspen works end to end, but it has not passed its
> own launch gate, there are no published releases, and storage formats may
> still change. Do not put data you cannot afford to lose in it yet. See
> [Pre-alpha status](#pre-alpha-status) for exactly what is and is not
> verified.

**Live demo:** <https://place.reasonmachines.com>, a shared pixel canvas
where every brush stroke is a `git push`
([mirror on GitHub](https://github.com/reason-machines/place-canvas)):

```bash
git clone https://place.reasonmachines.com/canvas.git
```

Aspen's design follows OpenAI's
"[Scaling Git at OpenAI](https://openai.com/index/scaling-git-at-openai/)",
which describes their internal Git service, Pando, and takes further
inspiration from Cursor's
"[Git at any scale](https://cursor.com/blog/git-at-any-scale)",
which describes the storage behind their Git server, Origin. Aspen is an
independent implementation, not affiliated with OpenAI or Cursor;
[Where the design comes from](#where-the-design-comes-from) says which idea
came from which. The launch post,
"[Aspen: scaling agentic commits beyond what Git was built for](https://reasonmachines.com/blog/aspen)",
walks through the design with interactive diagrams.

## Why

A classic Git server keeps each repository as a directory on one disk. Every
push takes a lock on the repository, and every fetch makes the server build a
new pack for that client. With a thousand agents pushing to their own
branches and CI fetching after every push, the server spends its time on
locks and on compressing the same objects again and again.

A Git server has to scale two ways: many writers pushing to different
branches, and many readers fetching the same history. Following Pando, Aspen
gives up two guarantees most Git servers treat as fixed:

- **One snapshot of every branch.** Each branch is a chain of versions in the
  database and moves on its own, with one compare-and-swap on that branch's
  tip: "move this branch from commit A to commit B, but only if it still
  points at A". Pushes to different branches never wait for each other. Two
  pushes to the same branch race on that row; one wins and the other gets
  Git's usual "fetch first". The cost: a reader looking at several branches
  may see each one at a slightly different moment.
- **A custom pack for every client.** Aspen never unpacks a push. It checks
  the pack, adds the objects Git left out because the server already had
  them, and stores it in object storage as one self-contained pack. A fetch
  streams the stored packs the client is missing, recompressing nothing, or
  hands out URLs so the client downloads them from the bucket, a pack cache
  or a CDN.

Two more choices keep this fast as a repository grows:

- **A push sees only the trunk branch**, so what a client reads before it
  pushes stays the same size however many branches exist, and anyone who
  has trunk can fetch the branch with the one pack the push sent.
- **Small packs are merged in the background** in levels, like an LSM tree,
  so a client thousands of pushes behind gets a few dozen packs, not
  thousands.

## Where the design comes from

Aspen takes its inspiration from two published write-ups, and does some
things its own way.

**OpenAI, "[Scaling Git at OpenAI](https://openai.com/index/scaling-git-at-openai/)"** (Aaron Friel, Taylor Blau
and Ted Nyman, November 2026) describes Pando, OpenAI's Git backend. Aspen
is an independent implementation of that design and shares no code with it.
From Pando:

- Per-branch (causal) consistency: each push records a ref version (the new
  tip, the pack that brought it, and the versions it depends on) in a
  metadata store, and the only contended write is a compare-and-swap on the
  branch's pointer.
- Pushed packs are kept as immutable blobs and reused to answer later
  fetches, instead of building a pack for each client.
- A push is advertised only the trunk branch.
- A push's metadata write is split into a synchronous part, so the new
  commit can be fetched at once, and asynchronous work recorded in the same
  transaction, so the database work per push stays bounded however large
  the push is.
- Fetch planning maps a client's haves and wants to ref versions and selects
  stored packs, which can be delivered as `packfile-uri` links through a
  regional pack cache. Pando calls its cache Express; Aspen's is
  `aspen-express`.
- Compaction in geometric levels, like a log-structured merge tree, with the
  levels doubling as a skip list over versions, so planning a fetch takes
  logarithmic time in how far behind the client is.
- Running as a read- and write-through proxy in front of an existing Git
  host while traffic moves over (Aspen's mirror mode), and checking behaviour
  against stock Git (Aspen's differential suite).

**Cursor, "[Git at any scale](https://cursor.com/blog/git-at-any-scale)"** (Vicent Martí, August 2026)
describes Continuity, the storage behind Cursor's Git server, Origin. From
Cursor:

- The bar a push is measured against: acknowledged only once it is durable
  in object storage, and counted in pushes a second to one repository. Aspen
  holds its own throughput to Origin's published numbers
  ([How fast](#how-fast)).
- Batch the write that publishes pushes, not the pushes' data. Continuity
  stores each push as its own object and batches the update that publishes
  them; Aspen's group commit (`ASPEN_GROUP_COMMIT`, off by default) likewise
  keeps one pack per push and batches the database transactions that
  publish concurrent pushes.

**Where Aspen differs:**

- **No repository-wide log.** Continuity orders every push to a repository
  through one write-ahead log in S3, which gives a consistent view of every
  ref. Aspen, like Pando, orders only pushes to the same branch, so no
  single log's compare-and-swap latency caps a repository's push rate, and
  there is no single snapshot of all branches.
- **No Git repository on disk.** Continuity keeps a normal Git repository on
  local NVMe as a cache and avoids a relational database. Aspen keeps no
  repository on disk: packs live in object storage and metadata in
  PostgreSQL (or SQLite on one machine), so any server can answer any
  request.
- **Stitched packs for stock clients.** Pando's post describes sending
  `packfile-uri` links only, with an empty inline pack. Aspen sends links to
  clients that opt in (`fetch.uriprotocols`) and streams everyone else the
  planned packs joined into one pack, so an unconfigured `git` is served
  without the server building a pack.
- **Self-contained packs.** A thin pushed pack is completed with its missing
  bases before it is stored, so every stored pack can be served on its own.
- **Force pushes and deletes take `-o force`**, a lease or the
  branch-scoped URL. OpenAI's Git fork adds an `explicit-haves` extension
  so plain `--force-with-lease` works; Aspen targets stock clients and does
  not implement it.
- **Exact shallow packs.** CI fetches with `--depth=1`, so a `--depth=N`
  fetch of a branch tip is served a shallow pack built once per version and
  depth and stored like any other pack.
- **A server-side merge queue** (`ASPEN_MERGE_QUEUE`, off by default) lands
  many changes, each its own commit, in one update of trunk.
- **One process for development:** `aspen dev` runs the whole server with
  SQLite and a local directory.

## How fast

These numbers are from the [launch post](https://reasonmachines.com/blog/aspen). On one 8-core server with
S3 Standard and a 4-core PostgreSQL, pushing to a copy of git.git (about
4,500 files and 75,000 commits) through Aspen's branch-scoped URL, Aspen
sustained 567 pushes a second for five minutes with no errors. Every push was
in S3 and PostgreSQL before Git printed `ok`. The median push took 208 ms and
the 99th percentile 314 ms, and another client saw each push 30 ms after it
was acknowledged, at the median.

| Pushes a second to one repository, each durable before it is acknowledged | Rate |
|---|---|
| Cursor Origin, S3 Standard (as published) | 120 |
| Cursor Origin, S3 Express One Zone (as published) | more than 300 |
| Aspen, S3 Standard (one 8-core server, git.git, branch-scoped URL) | 567 |

The setups differ: Cursor's numbers come from a replicated cluster serving
its own monorepo, whose size its post does not give. OpenAI did not publish
a push rate for Pando; it reports median push latency down by nearly 80%
against its previous system and nearly a petabyte of Git data served a day.

What the number does not cover:

- Each push still costs more as a repository grows, so very large monorepos
  see lower rates today.
- Landing on trunk is a different number. Each landing builds on the one
  before it, so separate landings run one at a time: about 4 a second,
  limited by round trips rather than load. The merge queue lands many
  changes in one trunk update; its rate has not been measured yet.
- None of these numbers include CI.
- The raw results of this run are not in this repository. The benchmarks you
  can reproduce from it are in [site/src/benchmarks.md](site/src/benchmarks.md)
  and [bench/campaign](bench/campaign/README.md).

## How it works

![How a push and a fetch flow through Aspen](docs/assets/architecture.png)

A push is verified, completed into a self-contained pack and uploaded to
object storage; then one compare-and-swap moves that branch's tip in the
metadata store. A fetch asks the metadata store which stored packs cover what
the client wants, and the stitcher streams them as one pack or hands out
packfile URIs for a CDN. Compaction merges small packs in the background.

It runs with one binary, PostgreSQL and any S3-compatible bucket, or with
SQLite and a local directory on one machine. Mirror mode puts Aspen in front
of GitHub or GitLab, so you can try it without migrating.

## Quickstart (5 minutes once built)

There are no published binaries yet, so you build from source. You need
`git`, a Rust toolchain from [rustup](https://rustup.rs) (the version is
pinned in `rust-toolchain.toml` and installed on first use) and a C compiler
(`build-essential` on Debian and Ubuntu, the Xcode command line tools on
macOS). The build is the slow part and can take over ten minutes on a
small machine.

```bash
git clone https://github.com/reason-machines/aspen
cd aspen
cargo install --locked --path crates/aspen-cli
aspen --version
```

Start a development server in an empty directory. `aspen dev` runs the whole
server in one process with SQLite and a local pack directory under `./.aspen`,
creates a repository named `demo`, and listens on `127.0.0.1:7070` only,
because anyone who can reach it may push.

```bash
mkdir aspen-try && cd aspen-try
aspen dev
```

In a second terminal, push, clone back, push a branch and make a shallow
clone:

```bash
git init -b main hello && cd hello
echo "hello, aspen" > README.md
git add README.md && git commit -m "first commit"
git push http://127.0.0.1:7070/demo.git main
cd ..
git clone http://127.0.0.1:7070/demo.git copy
git -C copy fsck --strict
cd hello
git switch -c feature
echo "more" >> README.md && git commit -am "on a branch"
git push http://127.0.0.1:7070/demo.git feature
git ls-remote http://127.0.0.1:7070/demo.git
cd ..
git clone --depth=1 http://127.0.0.1:7070/demo.git shallow
```

What you should see, with your own commit IDs in place of `<commit>`
(output from git 2.39):

```text
To http://127.0.0.1:7070/demo.git
 * [new branch]      main -> main
Cloning into 'copy'...
To http://127.0.0.1:7070/demo.git
 * [new branch]      feature -> feature
<commit>	HEAD
<commit>	refs/heads/feature
<commit>	refs/heads/main
Cloning into 'shallow'...
```

Stop the server with Ctrl-C; it finishes requests in flight first.

One thing behaves differently from other Git servers: because a push only
sees trunk, a force push needs `git push --force -o force` and a branch
delete needs `-o force` (or the branch-scoped URL). To delete the `feature`
branch from the quickstart:

```bash
git push -o force http://127.0.0.1:7070/demo.git --delete refs/heads/feature
```

[Using Git with Aspen](site/src/using-git.md) explains why.

If you push the same branch again and again (an agent pushing after every
change, say), turn on `push.negotiate` in the clones that push:

```bash
git config --global push.negotiate true
```

Because a push only sees trunk, Git otherwise assumes the server lacks
every commit of your branch that is not on trunk, and resends all of them
on every push: the 40th push of a 40-commit branch sends 40 commits. With
`push.negotiate`, `git push` first asks Aspen which of your commits it
already has, then sends only the new ones, so each push costs the same
however long the branch gets. In Aspen's tests the 40th push sent about
500 bytes instead of 15 KB, at the price of one extra request per push.
It needs protocol version 2 (Git's default since 2.26) and was tested with
Git 2.39. Aspen never stores a resent commit twice either way; the setting
saves the upload and the server's work reading it.

Next steps: [require tokens with `aspen serve`](site/src/getting-started.md#2-require-tokens),
run the [single-host stack](site/src/getting-started.md) with PostgreSQL and
MinIO (`docker compose up`), or deploy with the Helm chart or Terraform for
AWS, Google Cloud, Azure or Hetzner ([deploy/README.md](deploy/README.md)).

## Pre-alpha status

Before calling Aspen ready, the project set itself a launch gate
([docs/PLAN.md section 7](docs/PLAN.md#7-launch-gate-all-true-before-the-demo-is-public)).
Aspen is being published before that gate is met. Here is where each item
stands:

| Launch-gate item | Status |
|---|---|
| 24-hour chaos run with no lost acknowledged push | **Not done.** The run was stopped after 11.59 clean hours of mixed load in three segments (longest 8.01 hours), with no violations, all on a build from before the storage fix in #231. A continuous 24-hour run did not fit the test host's disk (#225, open, has the per-segment results). Required CI runs a short chaos smoke on every change. |
| Fuzzers ran 24 hours clean | **Not done.** The 13 fuzz targets run for 180 seconds each every night ([fuzz/README.md](fuzz/README.md)). |
| Benchmarks reproduced from scratch by a teammate following the docs | **Not done.** The [How fast](#how-fast) numbers come from one run on one 8-core server, and the reproducible campaign ([bench/campaign](bench/campaign/README.md)) from one 4-vCPU machine running server and clients together; treat both as indicative, not as performance claims. |
| `docker compose up` and AWS Terraform verified from a clean account using only the docs | **Not done.** The Terraform modules pass validation and mocked-provider tests in CI, and the compose stack is smoke-tested nightly, but nobody has followed the docs end to end in a fresh account. |
| Differential suite: 0 divergences over 1 million random operations | **Not done at that scale.** Required CI runs the suite against Aspen on every change and the nightly job runs thousands of operations; no divergences are known. |
| Interop client matrix all green | **Not signed off.** The matrix (git 2.30 to latest, JGit, libgit2, go-git, gitoxide) runs nightly against Aspen. |
| Public endpoint security review done; quotas and kill switch tested | **Review done** ([docs/SECURITY.md](docs/SECURITY.md)); quotas and the kill switch have not been tested on the deployed demo yet. One review finding is still open and must be fixed before the demo is public: bounding concurrent fetches, pushes and stitch work per caller and per instance (#118, being fixed). |
| Budget alarm live | **Pending** with the demo deployment. |
| Depth-1 fetch storm at most 1.2× stock `git upload-pack` bytes | **Measured:** 1.029× on git.git, 1.000× on a small synthetic repository ([site/src/benchmarks.md](site/src/benchmarks.md)). |

### Known limits

- No partial clones: `--filter` is not advertised, so `git clone --filter=...`
  falls back to a full clone.
- Shallow fetches other than `--depth=N` (N up to 64) of a branch tip are
  served the full history of what they ask for, with a correct shallow
  boundary.
- No SHA-256 repositories, no web UI, no code review or issues (keep those on
  GitHub or GitLab with mirror mode).
- `git fetch` does not follow new annotated tags on its own; `git fetch
  --tags` and `git clone` do.
- Force pushes and branch deletes need `-o force`, a lease or the
  branch-scoped URL (see above).
- SSH accepts Ed25519 and ECDSA keys only.
- Rate limits are per instance, in memory; there are no per-repository
  storage quotas.
- Object storage: S3 and S3-compatible stores and the local file system.
  Google Cloud Storage works through its S3 API with an HMAC key; Azure
  Blob Storage is not supported yet.
- Mirror mode: HTTP(S) upstreams only (no SSH; credentials only over HTTPS
  or to loopback), no Git LFS.
- Storage overhead from compaction and multi-ref pushes has not been measured
  on a large repository.

The full list, with the reason behind each, is
[site/src/limitations.md](site/src/limitations.md).

## Documentation

- [site/src](site/src): getting started, using Git with Aspen, self-hosting,
  configuration, architecture, operations and limitations. Build it as a
  site with `site/install-mdbook.sh && python3 site/build.py`.
- [deploy/README.md](deploy/README.md): the container image, Compose, Helm,
  Terraform and releases.
- [docs/PLAN.md](docs/PLAN.md): the design, the execution plan, its progress
  and a log of every design decision and why it was made. Aspen was built by
  a coding agent coordinating one agent session per piece of the plan, with
  every change reviewed and merged as a pull request; the plan is its record.
- [docs/SECURITY.md](docs/SECURITY.md): the threat model and security review.

## Repository layout

| Path | What |
|---|---|
| `crates/aspen-core` | Contracts every crate builds on: IDs, names, the commit check, store, pack and planner traits, errors |
| `crates/aspen-proto` | Git wire protocol: pkt-line, receive-pack, upload-pack v0 and v2 |
| `crates/aspen-pack` | Streaming pack ingest and indexing, thin-pack completion, the pack stitcher |
| `crates/aspen-meta` | The metadata store on PostgreSQL and SQLite, and its migrations |
| `crates/aspen-blob` | Object storage: S3-compatible stores and the local file system |
| `crates/aspen-planner` | Fetch planning: which stored packs a fetch needs |
| `crates/aspen-compact` | Background compaction and commit indexing |
| `crates/aspen-express` | A regional pack cache for packfile URIs |
| `crates/aspen-mirror` | Mirror mode for GitHub and GitLab |
| `crates/aspen-auth` | Tokens, OIDC, SSH keys, access policy, rate limits |
| `crates/aspen-server` | The HTTP and SSH server |
| `crates/aspen-cli` | The `aspen` binary |
| `testkit`, `tests` | Differential oracle against stock Git, interop, chaos suite |
| `fuzz`, `bench` | Fuzz targets; the `aspen-bench` load generator and baselines |
| `deploy` | Image, Compose, Helm, Terraform, release tooling, dashboards |
| `demo/place` | The `place` demo: web front end and its server |
| `site` | The documentation site (mdBook) |

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing and sending
changes. Please report vulnerabilities privately as described in
[SECURITY.md](SECURITY.md), not in public issues.

## License

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
