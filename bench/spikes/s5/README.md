# Spike S5: shallow and filter requests

**Question (from [docs/PLAN.md](../../../docs/PLAN.md), node S5):** how should
Aspen handle shallow fetches and partial-clone filters, and what do stock Git
clients do when the server doesn't advertise them?

**Short answer:**

- **Filters:** don't advertise `filter`. Every tested client falls back to a
  full fetch with a one-line warning. This confirms the plan.
- **Shallow:** don't strip or refuse shallow by default. Without it, the
  default fetch in GitHub Actions (`actions/checkout`, depth 1) and GitLab
  Runner (depth 20) fails on every tested client. v1 serves shallow requests
  by sending the same packs a full fetch would get, plus a correct
  `shallow-info` section. Clients accept this. It costs bandwidth, not
  correctness.
- **Two hazards N1 must design for:**
  1. The normal have-based planner is unsafe for clients that are already
     shallow: it fails their fetches.
  2. Lazy fetches from existing partial clones ask for blob IDs, which
     `commit_index` can't resolve.

The rest of this document is the evidence and the contract changes it implies.

## What was run

- **Clients:** git 2.30.0 and 2.56.0 (built from the tagged source), and
  git 2.39.5 (Debian 12). All over smart HTTP, with protocol v0 and v2.
- **Server:** stock `git http-backend` 2.39.5 behind
  [`capproxy.py`](capproxy.py), a small proxy that rewrites the capability
  advertisement and, in some modes, the response. Each server behaviour Aspen
  might adopt is one mode:

| Mode | Advertises | What it does with a shallow request |
|---|---|---|
| `stock` | shallow + filter | stock upload-pack (control) |
| `strip` | neither | nothing; records a violation if the client sends one anyway |
| `nofilter` | shallow | stock upload-pack: an exact, per-request shallow pack |
| `refuse` | shallow | `ERR <message>` pkt-line |
| `overfetch` | shallow | correct `shallow-info` + the full-history pack of the wants (v2 only) |
| `planner` | shallow | like `overfetch` for deepen requests. For an already-shallow client, it ignores the client's shallow lines and sends want minus everything reachable from the haves. That's what a planner selecting whole ref versions would do. |
| `tipsonly` | shallow | like `nofilter`, but only advertised tips may be wanted |

- **Scenarios:** 15, in [`matrix.py`](matrix.py), on a fixture of 30 commits
  with a tag at commit 10 and a topic branch forked at commit 25:
  - plain clone;
  - clone with `--depth 1`, `--shallow-since`, `--shallow-exclude`;
  - clone with `--filter=blob:none` and `--filter=tree:0`;
  - the exact fetch commands that `actions/checkout@v4` and GitLab Runner run
    by default;
  - from an existing depth-1 clone: `fetch`, `fetch --deepen=3`,
    `fetch --unshallow`, fetch a branch forked below the shallow boundary, and
    fetch a commit that restores a blob last seen below the boundary;
  - from an existing `blob:none` clone: `fetch`, and a lazy fetch triggered by
    checking out an old commit.

  Every run records the exit code, stderr, whether the result is shallow or a
  partial clone, its commit and object counts, and `git fsck --strict`.

- **Totals:** 630 runs (3 clients × 2 protocols × 7 modes × 15 scenarios). Full
  results:
  - [results/matrix.md](results/matrix.md): tables;
  - [results/wire-examples.md](results/wire-examples.md): what went over the
    wire, with the decisive client output quoted.

  Reproduce with `bench/spikes/s5/run.sh git /path/to/other/git`. It takes
  about 90 seconds for three clients. A run also writes
  `results/matrix.json` (every run, all clients, about 340 KB) and
  `results/transcripts/` (one file per run). Both are generated and
  gitignored, so they are not in the repository.

The three clients behaved identically in every cell except one old-client
quirk, noted below. No run failed `fsck --strict`. No client ever sent a
shallow, deepen or filter line that the server had not advertised.

## Findings

### 1. Filter not advertised: clients fall back to a full fetch

`git clone --filter=blob:none` (or `tree:0`) against a server that doesn't
advertise `filter`:

- prints `warning: filtering not recognized by server, ignoring`;
- sends no `filter` line;
- receives the full pack (157 of 157 objects);
- exits 0, and the clone passes `fsck`.

The same holds for v0 and v2 on all three versions.

One side effect: the clone is still configured as a partial clone
(`remote.origin.promisor=true`, `remote.origin.partialclonefilter=blob:none`,
and a `.promisor` file next to the pack). Later fetches repeat the warning.
This is harmless because no objects are missing, but the docs should say so.

**Consequence:** the plan's "don't advertise `filter`" is safe for stock Git.
If a request contains a `filter` line anyway, the client is non-stock or
buggy. Answer it with `ERR` instead of guessing.

### 2. Shallow not advertised: hard failure, including default CI

With shallow capabilities stripped, every shallow operation fails before any
pack is sent. The client produces the error itself:

| Request | v0 | v2 |
|---|---|---|
| `--depth N`, `--deepen`, `--unshallow`, any fetch from a shallow clone | `fatal: Server does not support shallow clients` | `fatal: Server does not support shallow requests` |
| `--shallow-since` | `fatal: Server does not support --shallow-since` | same as above |
| `--shallow-exclude` | `fatal: Server does not support --shallow-exclude` | same as above |

That includes:

- **`actions/checkout`'s default fetch:**
  `git fetch --no-tags --prune --no-recurse-submodules --depth=1 origin +<sha>:refs/remotes/origin/main`
- **GitLab Runner's default fetch:** `--depth 20`.
- **Existing shallow clones:** anyone who points one at Aspen can no longer
  fetch at all.

CI is the traffic Aspen is built for, so stripping shallow is not acceptable
as the default.

### 3. Refusing with `ERR` works and shows our message verbatim

Answering a shallow request with an `ERR <msg>` pkt-line makes every client,
in both v0 and v2, print `fatal: remote error: <msg>` and exit 128, with
nothing written. That is the right tool for an operator who wants to forbid
shallow (a `refuse` policy), and for the request shapes Aspen chooses not to
implement. It does not fix the CI breakage in finding 2.

### 4. Over-fetch is accepted: full-history pack plus exact `shallow-info`

In `overfetch` mode, the server:

- computes the exact `shallow-info` (the same shallow/unshallow lines stock
  upload-pack would send);
- then sends the pack a client with no haves would get for those wants.

That is the closure of the wants, which Aspen can already serve as stored or
stitched packs.

With protocol v2, all three clients accept this for every shallow scenario:
depth, since, exclude, the CI commands, deepen, unshallow, and fetches from
shallow clones. In each case:

- `git rev-parse --is-shallow-repository` and `git rev-list --count` report
  exactly what stock gives;
- the extra objects sit in the pack unused;
- `fsck --strict` passes.

The cost is bandwidth. In this fixture:

| Operation | Over-fetch | Exact shallow pack |
|---|---|---|
| `--depth 1` clone | 502 KB | 17 KB |
| Incremental fetch from a depth-1 clone | 518 KB | 17 KB |

On a large monorepo the ratio is the full clone size divided by one tree.

Protocol details that matter for the implementation:

- If the client has not sent `done`, it expects an `acknowledgments` section
  first. Without it, git fails with
  `expected 'acknowledgments', received 'shallow-info'`. Sending
  `acknowledgments` / `ready` / delim before `shallow-info` fixes it.
- An empty `shallow-info` section (header only) is valid. Stock upload-pack
  sends one when a shallow client fetches without deepening.
- For `--depth 1`, `shallow-info` is just `shallow <oid>` for each wanted
  commit that has parents.
  - Depth N > 1 needs a parent walk.
  - `--shallow-since` also needs committer times.
  - `--shallow-exclude` also needs ref resolution.

### 5. The normal planner is unsafe for clients that are already shallow

A shallow client that fetches sends:

- `shallow <oid>` lines for its boundary;
- `have` lines for the commits it holds.

A planner that maps those haves to ref versions skips every object reachable
from them, including history below the client's boundary, which the client
doesn't have. The `planner` mode models this. It uses a reachability bitmap,
because without one stock pack-objects excludes only the boundary commits'
trees and hides the problem.

Two scenarios fail on all three clients:

| Scenario | Client error |
|---|---|
| Fetch a commit that restores a blob last present below the boundary | `fatal: missing blob object '757cc08…'` / `did not send all necessary objects` |
| Fetch a branch forked below the boundary | `error: Could not read e20babe…` / `fatal: revision walk setup failed` / `did not send all necessary objects` |

In both cases the client detects the gap and rejects the fetch, so nothing is
corrupted, but the fetch fails. The ordinary shallow `fetch` happened to work,
which is why this needs a test rather than a spot check.

**Consequence:** when a request carries `shallow` lines, the planner must not
use its haves the usual way. The simple correct rule for v1: ignore the haves
and serve the full closure of the wants, as in finding 4. A smarter rule can
come later, but it must pass both of these scenarios.

### 6. Existing partial clones send blob-ID wants on lazy fetch

A partial clone made against a filter-capable server (for example, GitHub,
before someone points it at an Aspen mirror) keeps working for ordinary
fetches. Checking out an old commit triggers a lazy fetch:

```
command=fetch
thin-pack
no-progress
ofs-delta
want <blob-oid>
... (one want per missing blob)
done
```

There are no haves and no filter line. The wants are **blob IDs**.

- **v2:** stock upload-pack serves these whatever `uploadpack.allow*SHA1InWant`
  says.
- **v0:** it refuses them unless `allowAnySHA1InWant` is set, with
  `error: Server does not allow request for unadvertised object <oid>`. Then:
  - git 2.39 and 2.56 exit 128 (`could not fetch … from promisor remote`);
  - git 2.30 prints the errors but exits 0 with the checkout incomplete. This
    is the one cell where the clients differ.

Aspen's `commit_index` maps commit IDs only, so it can't resolve these wants.

**Consequence:** a want that is not a known commit (or annotated tag) needs an
explicit outcome:

- **v1:** `ERR` with a message telling the user that lazy fetching isn't
  supported, and to re-clone without `--filter`.
- **Mirror mode (C4):** forward unresolvable wants to the upstream.
- **Later:** an object-to-pack lookup through the stored packs' `.idx` files.

## Decision

1. **Filters.** Never advertise `filter`, in v0 or v2. Answer a `filter` line
   with `ERR aspen: partial clone filters are not supported; clone without --filter`.
   Document the leftover `promisor` config and the warning.
2. **Shallow is served in v1, by over-fetching.**
   - Advertise `shallow` in v2 `fetch=` and in v0 (`shallow deepen-since deepen-not deepen-relative`).
   - For any request with deepen or shallow lines, ignore the haves.
   - Plan the full closure of the wants, the same packs a fresh full fetch
     gets, through the normal stitched or `packfile-uri` path.
   - Send an exact `shallow-info` before the pack, preceded by
     `acknowledgments` / `ready` when the client hasn't sent `done`.
   - This keeps "never build a pack per client" and makes every tested shallow
     operation succeed.
   - The cost is full-clone bandwidth per shallow fetch. Record it in the docs
     and the benchmarks.
3. **Exact shallow packs are a later optimisation, not v1.** "Materialise once
   per (want set, deepen spec), store as an ordinary pack, serve it to every
   later identical request" fits the fetch-storm case (1,000 CI jobs, one
   commit, depth 1). It is node C8 in the PLAN (after B2 and C1), and the depth-1
   fetch-storm benchmark in PLAN section 4 tracks the bandwidth gap until then.
4. **Operator policy `shallow = serve | refuse`, default `serve`.** `refuse`
   answers with `ERR` (finding 3). The message must say what to do.
5. **Non-commit wants are answered with `ERR` in v1.** Mirror mode may proxy
   them upstream later (C4).

The PLAN.md "Not in v1" entry and the decisions log are updated to match.

## What N1 should take from this

- **`aspen-core` fetch request type:** carry, as parsed fields rather than raw
  lines:
  - `wants: Vec<ObjectId>`. These may name non-commit objects; resolution can
    fail.
  - `haves`.
  - `client_shallow: Vec<ObjectId>`.
  - `deepen: Option<Deepen>`, where
    `Deepen = Depth(u32) | Since(timestamp) | Not(Vec<RefName>)`.
  - `deepen_relative: bool`.
  - `filter: Option<String>`. This is always rejected, but parse it so it can
    be refused clearly.
- **Planner contract:** when `client_shallow` is non-empty or `deepen` is set,
  the planner gets no haves (finding 5). Property tests (A5) should include the
  "resurrected blob" and "branch below the boundary" cases against the
  `git rev-list --objects` oracle, with the client's shallow file applied.
- **Response type:** an optional `ShallowInfo { shallow, unshallow }` emitted
  before `packfile-uris` and `packfile`. The `acknowledgments` section with
  `ready` is required whenever the client didn't send `done`.
- **Error type:** an `Unsupported { feature, message }` variant that
  `aspen-proto` renders as an `ERR` pkt-line. All tested clients show it
  verbatim.
- **Schema v1: `commit_index` needs parent IDs and committer time** (a
  generation number is optional). Computing `shallow-info` for depth N, since
  and exclude needs a parent walk. The push path's fast-forward check (PLAN
  section 2, step 3) needs ancestry too. Storing the commit graph in the
  database lets both run without reading packs. Depth 1 needs only a
  "has parents" bit.
- **Unindexed commits during a walk:** `commit_index` rows for non-tip
  commits of a large push are written by a background job, so a parent walk
  (depth N, `deepen-since`, `deepen-not`, `deepen-relative`, or the push
  path's fast-forward check) can reach a commit that has no row yet. The
  contract must make that state explicit rather than treat a missing row as
  "no parents" or "not an ancestor":
  - the commit-graph lookup returns `Indexed(parents, time) | NotIndexed`;
  - on `NotIndexed`, the caller reads the commit object from the packs of the
    ref version that reached it (its `pack_id`, then its `deps`). Every
    ancestor of a ref version's tip is in that set, so this always succeeds
    on healthy storage;
  - if the commit is in neither the index nor those packs, the request fails
    with a retryable error (`ERR` for fetch, `ng` for push). It never emits
    a `shallow-info` or a fast-forward verdict computed from a partial walk.
  A5's property tests should run the planner and the shallow-boundary walk
  with a randomly incomplete `commit_index` and check the result equals the
  fully indexed one.
- **Capabilities:**
  - v2: `fetch=shallow` (plus whatever else B2 supports), never `filter`.
  - v0: `shallow deepen-since deepen-not deepen-relative`, never `filter`.

## Not covered (follow-ups for the nodes that own them)

- **Over-fetch over v0 was not modelled.** Response order in v0 is
  shallow/unshallow lines, flush, ACK/NAK, pack. B2 must prove it with the v0
  interop runs.
- **Only the git CLI was tested.** JGit, libgit2, go-git and gitoxide belong
  to the nightly interop matrix (section 4).
- **SSH was not tested.** It uses the same pkt-line exchange, but it is
  unverified here.
- **`shallow-info` combined with `packfile-uris` was not tested.** The
  protocol allows it (`shallow-info` comes before `packfile-uris`). B2/S3
  should cover it.
- **The server side here is stock git behind a proxy, not Aspen.** The
  results show what clients accept. They don't show Aspen's implementation
  works.
