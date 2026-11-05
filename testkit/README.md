# aspen-testkit

Test tooling for Aspen. Nothing here ships in the server; product crates may
use it only as a dev-dependency. `cargo xtask check-deps` enforces that: it
lists `aspen-testkit` among the tooling crates, and a normal dependency on one
from a product crate fails with "product crate ... depends on tooling crate
aspen-testkit; make it a dev-dependency".

| Piece | What it is |
|---|---|
| `git` module | Isolated `git` invocations: no system or global config, no prompts, fixed identity |
| `GitHttpBackend` | A std-only HTTP server that hands each request to stock `git http-backend` (the reference server); optionally with Aspen's branch-scoped ref visibility |
| `GitServer` trait | "Server under test": create an empty repository, give each branch's URL, list every ref |
| `StockServer` | `GitServer` backed by `GitHttpBackend` and a temporary directory; every branch on one URL |
| `ScopedServer` | Stock Git in Aspen's URL layout: only trunk on the repository URL, each branch on its own URL |
| `AspenServer` | A real Aspen: the `aspen` binary running `aspen serve` (SQLite or PostgreSQL, local blob store) |
| `Oracle` | The differential oracle from section 4.1 of [`docs/PLAN.md`](../docs/PLAN.md) |
| `FaultyServer` | Deliberately broken servers that prove the oracle catches real bugs |
| `images/` | Client-matrix Docker images and `run-matrix.sh` |

## Differential oracle

For each seed, the oracle generates a random sequence of operations and
applies it, step by step, to a fresh repository on a *reference* server and
a *candidate* server, through stock `git` clients over HTTP (protocol v2 for
fetches). Operations: push (1-3 commits), force-push, delete, create a branch
from another, merge with a merge commit, fetch, concurrent pushes from
several clones to distinct branches, and races of several clones pushing the
same branch at once. Several clones act on each repository, and each sees
the server only as of its last fetch, so stale pushes get rejected just as
they would for real users.

Aspen shows only the trunk on a repository's URL; each other branch is
pushed and fetched through its own branch-scoped URL,
`<repo>/-/refs/heads/<branch>` (spike S2). So the oracle never relies on one
URL showing everything. Each clone has one remote per branch
(`b-<branch>`, URL from `GitServer::branch_url`), every push and fetch of a
branch goes through it, and a fetch refreshes branches one URL at a time
(listing first, so a deleted branch is pruned). Force-pushes send
`-o force`, Aspen's force intent; stock servers ignore it. With
`protect_trunk` (the default) sequences never delete or force-push `main`,
which Aspen refuses by policy.

Most Aspen users push a topic branch through the plain repository URL, not
its branch URL. There Aspen advertises only trunk, so the client sends a zero
old value, cannot refuse a non-fast-forward itself, and Aspen decides alone:
its own fast-forward check, the `-o force` and `-o lease=` rules, and
`ng ... non-fast-forward` for a stale push. `Config::plain_url_pushes`
(`--plain-url-pushes`) exercises that path (#135):
it adds two ops that push (never fetch) through a `plain` remote whose URL is
the repository URL, a push or force-push (`Op::PlainPush`) and a race
(`Op::PlainRace`). On stock they are the same pushes as through the branch
URL. Outcomes are still compared as accepted or rejected per ref, so a stale
push that stock's client refuses (`fetch first`) and Aspen refuses (`ng`)
agree, and a plain-URL racer that loses must be rejected with a
`RACE_LOSS_REASONS` phrase or as non-fast-forward. It is off by default, so
earlier seeds replay unchanged (a test pins seed `0xa5be0000`), and it
changes the generated sequences, so a seed replays only with the same
setting; divergence reports print it in the replay command. `--candidate
scoped` refuses it: `ScopedServer`'s repository URL refuses updates of
hidden refs.

After every step it checks that:

1. the clients saw the same outcome (each ref update accepted or rejected the
   same way; message wording is ignored);
2. `GitServer::list_refs` (every ref, hidden or not) gives the same refs at
   the same object IDs;
3. fresh repositories that fetch every listed ref through its branch URL hold
   the same set of reachable objects;
4. `git fsck --strict` passes on both of those repositories.

A race may be won by a different clone on each server, which is not a bug.
So for a race the oracle checks invariants on each server instead of
comparing winners: exactly one push is accepted, the branch tip is that
push's commit, and every other push is rejected as non-fast-forward or for a
lost compare-and-swap. A server-side rejection counts only if its reason
contains one of the phrases in `RACE_LOSS_REASONS` (`src/world.rs`); stock
Git says "failed to update ref" or "cannot lock ref", and Aspen's `ng`
reasons must use one of those phrases too. If the winners differ, it
force-pushes the candidate's winner to the reference through the branch URL
and logs a `resync:` note for that step. The candidate is never asked to
rewrite a branch, so a race on trunk works against a server that refuses
forced trunk updates (Aspen's default).

Commit contents, messages and dates derive from the step number, so two
correct servers end up with byte-identical objects.

A divergence report names the seed, the step, what differed, the sequence up
to that step, a shrunk sub-sequence that still diverges (greedy, one op at a
time, bounded replays), and the command that replays it:

```text
differential oracle: faulty(LastWriterWins) diverged from git-http-backend
  seed: 0x0000000000000002 (steps=12, clones=3, branches=4, protect_trunk=true)
  first divergence after step 3: race on topic-2: clones 1, 2 push at once
  what: faulty(LastWriterWins) broke a race invariant:
      2 of 2 racing pushes to topic-2 were accepted, expected exactly 1: clone 1 (01938c115a69), clone 2 (885202a042de)
      topic-2 kept clone 1's commit 01938c115a69; the other acknowledged pushes were lost
  sequence up to the divergence:
      0: clone 1: push 1 commit(s) to topic-3
      1: clone 1: push 3 commit(s) to topic-2
      2: clone 0: force-push topic-3
      3: race on topic-2: clones 1, 2 push at once
  shrunk to 1 op(s), diverging after step 0: ...
      0: race on topic-2: clones 1, 2 push at once
  replay: cargo run -p aspen-testkit -- diff --seed 0x2 --sequences 1 --steps 12 --clones 3 --branches 4 --candidate <server>
```

Seeds use a small generator defined in this crate (SplitMix64), and a test
pins its output, so a printed seed keeps replaying across machines and
dependency upgrades.

### Running it

```bash
# Stock vs stock: any divergence is a bug in the oracle itself.
cargo run -p aspen-testkit -- diff --sequences 20 --steps 30

# Stock vs Aspen's URL layout: must also find nothing. --log-steps prints
# each step and each resync.
cargo run -p aspen-testkit -- diff --candidate scoped --log-steps

# Against a deliberately faulty server: must exit 1 with a report.
cargo run -p aspen-testkit -- diff --seed 1 --candidate fault-drop-ref
cargo run -p aspen-testkit -- diff --seed 2 --candidate fault-last-writer-wins

cargo run -p aspen-testkit -- diff --help
```

Without `--seed`, a random seed is chosen and printed. The tests in
`tests/oracle.rs` run in required CI (about a minute); the nightly workflow
runs thousands of operations with a fresh seed each night.

### Against a real Aspen

`AspenServer` runs the `aspen` binary built from the same checkout as a
child process, `aspen serve` with the deployment's environment contract
(`deploy/README.md`): metadata in a fresh SQLite file, or in PostgreSQL
given by URL; packs in a local blob store; an anonymous `write:*` policy.
The testkit does not link Aspen, so it stays a std-only tool.

- `create_repo` runs `aspen repo create <namespace>/<name>` (trunk `main`,
  force policy `branches`), under a namespace unique to the server, so runs
  can share one PostgreSQL database.
- `branch_url` is the branch-scoped URL, `<repo>.git/-/<refname>`.
- `list_refs` is protocol v2 `ls-refs` on the repository URL, which lists
  every ref from the metadata store (only push and v0 advertisements are
  narrowed to trunk).

```bash
cargo build -p aspen-cli -p aspen-testkit
target/debug/aspen-testkit diff --candidate aspen --plain-url-pushes

# On PostgreSQL: a throwaway cluster from the local binaries
# (ASPEN_TEST_PG_BIN, or /usr/lib/postgresql/*/bin).
url=$(testkit/scripts/throwaway-postgres.sh /tmp/aspen-pg)
target/debug/aspen-testkit diff --candidate aspen --aspen-database-url "$url" --plain-url-pushes
testkit/scripts/throwaway-postgres.sh --stop /tmp/aspen-pg
```

`--aspen-bin` (or `ASPEN_BIN`) picks another binary; by default it is the
`aspen` next to `aspen-testkit`. Trunk stays protected (no
`--delete-trunk`): Aspen refuses trunk deletes and forced trunk updates by
default (spike S2). If `aspen serve` dies, the next check fails with the end
of its log.

Required CI runs a fixed-seed slice in `crates/aspen-cli/tests/differential.rs`,
with `plain_url_pushes` on, plus a fixed sequence of plain-URL pushes (a
stale push, a forced one, a race, a fast-forward); Cargo builds the binary
for it. The nightly workflow runs thousands of operations on SQLite and on
PostgreSQL, with `--plain-url-pushes`.
`testkit/scripts/check-aspen-mutations.py` shows the run fails when Aspen
misbehaves: it rebuilds `aspen` with one fault at a time (a push stores its
pack minus one object; the stale-old check is skipped, so a racing push
overwrites the winner; a fetch plan leaves out a needed pack; a
non-fast-forward through the plain URL is accepted without `-o force`) and
expects a divergence from each.

### Known divergences

A divergence that is understood and filed can be stepped over, so a run
goes on to find the next one: `--known <name>` on the command line,
`Oracle::known_divergences` in code. Each `KnownDivergence` matches one exact
pattern and names its issue. The servers' state is still compared after the
step, the acting clone's view of the branch is set to the reference clone's
(so its next push builds on the same history), and every step stepped over
is counted in the report (`Stats::known`) and logged with `--log-steps`.
Nothing is stepped over by default.

There are no known divergences today, so `--known` accepts no value. The
last one, a delete of a ref that does not exist, which Aspen refused and
stock Git accepts as a no-op, was fixed in
#133. A new one gets
a `KnownDivergence` variant, a `--known` value and a row in a table here;
when its issue is fixed, delete them, and the run proves the fix.

### Plugging in another server

Implement `GitServer`: `create_repo` makes an empty repository that accepts
anonymous pushes and returns its URL, `branch_url` returns the URL that
advertises a ref, and `list_refs` reads the server's full ref listing (not
one URL's advertisement). Then:

```rust
let reference = aspen_testkit::StockServer::start()?;
let server = MyServer::start()?;
let oracle = aspen_testkit::Oracle::new(&reference, &server, aspen_testkit::Config::default());
let report = oracle.check_seeds(aspen_testkit::oracle::seeds(base_seed, 100))?;
assert!(report.divergence.is_none(), "{}", report.divergence.unwrap());
```

## Client matrix

`images/` holds one Docker image per client. Each runs the same scenario
against a server URL: clone, commit, push a branch, clone that branch again,
fetch incrementally. `run-matrix.sh` serves a repository with
`aspen-testkit serve`, seeds it, runs each client, and checks the results
with stock Git (refs on the server, parent of the pushed commit, and
`git fsck --strict` on a fresh clone).

| Client | Image | How |
|---|---|---|
| git 2.30.9, 2.39.5, 2.43.7, 2.47.3, latest | `images/git` | Built from the kernel.org release tarball; `latest` is resolved at build time |
| JGit 7.8.0 | `images/jgit` | `org.eclipse.jgit.pgm` command line on Temurin 21 |
| libgit2 | `images/libgit2` | pygit2 1.20.1, whose wheels bundle libgit2 (1.9.7) |
| go-git v5.19.3 | `images/go-git` | A small Go program, `main.go` |
| gitoxide 0.61.0 | `images/gitoxide` | `gix` release binary. `gix` cannot push yet, so it clones and fetches only |

```bash
testkit/images/run-matrix.sh                 # every client, in Docker
testkit/images/run-matrix.sh git-2.30 jgit   # some clients
testkit/images/run-matrix.sh --native        # clients installed on this machine, no Docker
```

`--native` runs the same scenario files with whatever is on `PATH` (`git`,
`jgit`, `python3` with pygit2, `go` or `$GO_GIT_SCENARIO`, `gix`); set
`GIT_BUILDS` to a directory of `git-<version>` install prefixes to test
specific Git versions without Docker. `--url` points the matrix at another
server instead of starting one.

`--aspen BIN` runs the matrix against a real Aspen: `BIN dev` (SQLite and a
local blob store) on loopback with a `matrix` repository. Aspen's repository
URL advertises only trunk, so each scenario gets a third argument, the URL
to clone the pushed branch through: Aspen's branch-scoped URL. libgit2 and
go-git fetch with protocol v0 and need it; Git, JGit and gitoxide use v2,
whose `ls-refs` lists every ref.

```bash
cargo build -p aspen-cli
testkit/images/run-matrix.sh --aspen target/debug/aspen             # every client, in Docker
testkit/images/run-matrix.sh --native --aspen target/debug/aspen git-latest
```

The nightly workflow builds every image and runs the matrix, one job per
client and server (stock `git http-backend` and Aspen).
