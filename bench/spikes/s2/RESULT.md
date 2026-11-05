# Spike S2 result: push semantics with a trunk-only advertisement

**Question.** Aspen's receive-pack advertises only the trunk branch (Pando's
design: a push pack then depends on trunk alone, so any reader holding trunk
can fetch the branch with that one pack). What do stock Git clients actually
do for creates, updates, force pushes, deletes, `--force-with-lease` and
atomic pushes when every other branch is hidden, and which server rules make
those operations correct?

**Answer in one paragraph.** Creates, fast-forward updates and atomic pushes
work unchanged with every client tested. Three things break, all on the
client side, because the client never sees the branch it is updating: the
server cannot tell `git push --force` from a plain push; `git push --delete
<short-name>` is refused by the client before it contacts the server; and
`--force-with-lease` is always refused by the client as "stale info". Each
has a working route with stock clients: a `force` push option, deleting by
full ref name (gated like a force, because it carries no old-oid), and a
`lease` push option or a branch-scoped push URL. Two
findings change the design: advertising a few older trunk tips as `.have`
lines stops clients that are behind trunk from pushing their whole history,
and because smart HTTP is stateless, a push's trunk dependency must be fixed
when the push commits, which is only sound while trunk is fast-forward-only.

Decisions are recorded in [docs/PLAN.md](../../../docs/PLAN.md) (push path
and decisions log). This file holds the evidence and the full rule set for
N1 and B1; [For N1](#for-n1) lists what the contracts must carry.
Decisive wire excerpts are in [wire-examples.md](wire-examples.md).

## Method

- [`server.py`](server.py) is a throwaway smart-HTTP server. Its receive-pack
  advertises trunk (`refs/heads/main`) only, indexes the pushed pack into a
  quarantine directory, applies the commit-check rules proposed below, and
  moves refs with one compare-and-swap `git update-ref --stdin` transaction
  per ref (one per push for `--atomic`). Fetches go to stock `git
  http-backend`. For every push it logs the commands, push options, pack
  object count, thin delta bases, objects the server already had, and
  **objects needed beyond current trunk that the pack does not carry** (zero
  means a reader holding current trunk can use the pack alone).
- [`run.py`](run.py) runs 30 scenarios, each against a fresh repository and
  clone, with fixed commit dates so object IDs are identical across runs. It
  captures client output, the server's view and the client's
  `GIT_TRACE_PACKET` trace.
- Clients: git **2.30.9**, **2.39.5** (Debian 12 system git), **2.56.0**
  (latest release on 2026-11-03), and **OpenAI's fork** (`openai/git` codex
  branch at `9fe64f46b`; reports 2.55.0.1134 from a full clone, `2.55.GIT`
  from the depth-1 clone `build-clients.sh` makes), which adds
  `explicit-haves`.
  Built with [`build-clients.sh`](build-clients.sh). Transport: smart HTTP,
  protocol v0 (Git uses v0 for push even when v2 is configured).
- Outputs: [`results/matrix.md`](results/matrix.md) (committed: one row per
  push step, one column per client, plus pack contents) and one transcript
  per client and scenario under `results/<client>/` (about 120 files, not
  committed; the lines this file relies on are quoted in
  [wire-examples.md](wire-examples.md)).

Reproduce: `./build-clients.sh && ./run.py --out results git-2.30=clients/2.30.9/bin/git git-2.39=/usr/bin/git git-2.56=clients/2.56.0/bin/git openai=clients/openai/bin/git`
(about two minutes after the clients are built). The matrix was regenerated
this way after the delete rule changed in review; every row other than the
delete rows (09, 16) came out identical to the first run.

## What the clients do

All four clients produced the same outcome in every scenario except 25
(`explicit-haves`, which only the fork implements).

| Operation (unadvertised branch) | What the client sends or does | Evidence |
|---|---|---|
| Create | `0000… <new> refs/heads/x`, pack built against trunk only | 01 |
| Fast-forward update | Same as create: **old-oid is zero**, because the client thinks the branch is new. Pack carries every commit since trunk again (8 objects for 2 commits) | 02 |
| Re-push of an unchanged branch | Still sends a create plus a pack; stock servers would say "Everything up-to-date". The client prints `[new branch]` every time | 03 |
| `git push --force` | **Byte-identical to a plain push.** The `+` only disables a client-side check that cannot run anyway | 04, 05 |
| `git push -o force` | Push option reaches the server (`push-options` must be advertised); works without `--force` | 06, 07 |
| `git push --delete x` / `:x` | **Refused by the client**: `error: unable to delete 'x': remote ref does not exist`. Nothing is sent | 08 |
| `git push --delete refs/heads/x` / `:refs/heads/x` | Sends `0000… 0000… refs/heads/x` with no pack: an unconditional delete. Aspen gates it like a force (decision 2) | 09, 16 |
| `--force-with-lease=x:<oid>` and `--force-with-lease` | **Refused by the client** as `(stale info)`: the expected value never matches an absent ref | 10, 11 |
| `--atomic` | `atomic` capability honoured; client accepts all-or-nothing results | 14–16 |
| `git push --tags` | Sends a create for every local tag on every push | 18 |
| `--prune`, `--mirror` | Cannot see unadvertised refs, so never delete them. `--mirror` also creates `refs/remotes/*` on the server | 28 |

Trunk itself is advertised, so it behaves like any Git server: the client
sends the real old-oid and refuses a non-fast-forward unless `--force` (17).

### Pack sizes and dependencies

| Scenario | Pack | Objects needed beyond trunk, missing from pack |
|---|---|---|
| 19 Client 20 commits behind trunk creates a branch | **16 objects, 12 already on the server** (its whole history) | 0 |
| 20 Same, server also advertises 8 earlier trunk tips as `.have` | **4 objects** | 0 |
| 21 Stacked branch `b` on `a`, trunk-only (3 pushes) | 12 → 16 → 20 objects | 0 |
| 22 Same with a full advertisement (stock server) | 12 → 4 → 4 objects | **11, 15** (depends on branch `a`) |
| 29 Branch edits a 200-line trunk file | 413 B, 1 thin delta base (a trunk blob) | 0 |
| 30 Same, server advertises `no-thin` | 806 B, no external bases | 0 |
| 26 Advertised trunk is one commit behind real trunk | 8 objects | 0 |
| 27 Advertised trunk was rewound away before the push landed | 4 objects, 2 thin bases | **2** |

## Decisions

### 1. Advertisement

- Advertise trunk as the only named ref, followed by up to **8 earlier trunk
  tips as `.have` lines**, log-spaced over trunk's ref versions (1, 2, 4, …
  back). A client that has not fetched the latest trunk still finds a common
  base instead of pushing its whole history (19 vs 20). All `.have` tips are
  trunk ancestors, so the pack's dependency is still the trunk version
  (decision 3).
- An empty repository advertises `capabilities^{}` (23).
- Capabilities: `report-status delete-refs side-band-64k quiet atomic
  ofs-delta push-options object-format=sha1 agent=aspen/<version>`. Do not
  advertise `no-thin` by default (decision 6).
- Send rejection reasons in the `ng` line **and** on side-band 2 even when the
  client asked for `quiet`. Clients request `quiet` whenever stderr is not a
  terminal (every agent and CI job), and `quiet` is meant to suppress
  progress, not errors.

### 2. Commit check

Inputs per command: `old`, `new`, the ref's current tip `cur` read in the
same transaction, whether the ref was advertised to this client (trunk, or
the ref named by a scoped URL), the push options, and the repository's force
policy `allow_force ∈ {none, branches, all}` (default `branches`: anything
except trunk).

| Case | Rule |
|---|---|
| `new = 0`, `cur` absent | reject `no such ref` |
| `new = 0`, ref is trunk | reject (trunk cannot be deleted) |
| `new = 0`, `old ≠ 0` and `old ≠ cur` | reject `stale old-oid` |
| `new = 0`, `old = cur` | compare-and-swap delete (scoped URL or lease); no force needed |
| `new = 0`, `old = 0`, `-o force` or `-o force=<ref>`, `allow_force` covers the ref | unconditional delete |
| `new = 0`, `old = 0`, otherwise | reject `delete of a ref this client has not seen needs -o force, -o lease=<ref>:<oid>, or the branch-scoped URL`. A zero old-oid delete is as blind as an invisible `--force` |
| `old ≠ 0` (client saw the ref) and `old ≠ cur` | reject `ref moved` (the compare-and-swap) |
| `old ≠ 0`, `new` descends from `cur` | fast-forward |
| `old ≠ 0`, not a descendant | the client saw the tip and still sent this, so it forced or leased: accept if `allow_force` covers the ref |
| `old = 0`, `cur` absent | create |
| `old = 0`, `cur = new` | no-op: report `ok`, write no ref version, discard the pack |
| `old = 0`, ref was advertised | reject `ref exists` (a stock client cannot produce this) |
| `old = 0`, tag | reject `tag already exists` unless `-o force` and policy allows |
| `old = 0`, `new` descends from `cur` | fast-forward (the client could not check, so the server does) |
| `old = 0`, not a descendant, `-o force` or `-o force=<ref>` | force if policy allows |
| `old = 0`, not a descendant | reject `non-fast-forward (fetch and rebase, or push with -o force)` |

Push option `lease=<ref>:<oid>` (empty `<oid>` means "must not exist")
replaces `old` for that ref before the table applies, and a matching lease
counts as an explicit force. `--atomic`: if any command is rejected, reject
the rest with `atomic push failed`; apply all accepted commands in one
metadata transaction, taking ref rows in name order to avoid deadlocks.

**Verdicts and the compare-and-swap.** `cur` is not just a tip: it is one
exact `ref_version` of the ref, and every verdict above (fast-forward,
lease, delete, no-op) is computed against it. The commit transaction's
compare-and-swap expects that same version. If the swap loses because the
ref moved, B1 either recomputes the verdict against the new version (a
bounded number of times; a fast-forward of a moved branch can still be a
fast-forward) or returns a retryable `ng`. It never commits a verdict
computed against a version other than the one it replaces. The spike
serialised pushes per repository with a lock, so it did not exercise this.

Writable namespaces default to `refs/heads/*` and `refs/tags/*`; reject
others (for example the `refs/remotes/*` that `--mirror` pushes, 28) with a
clear reason. Configurable per repository.

**Fast-forward check without a history walk (inference for B1, not exercised
in Aspen code).** Because the pack was built against trunk, it contains every
commit reachable from `new` that is not in trunk. So `cur` is an ancestor of
`new` iff either `cur` is one of the commits reachable from `new` inside the
pack, or `cur` is a trunk commit at or before a trunk commit that the pack's
boundary commits point to. With a fast-forward-only trunk, "at or before" is a
comparison of trunk ref-version sequence numbers through `commit_index`. The
spike used `git merge-base --is-ancestor` as the oracle.

### 3. Pack dependencies (input for N1's `ref_versions.deps`)

Smart HTTP is two requests: the advertisement (GET) and the push (POST). The
server does not know at POST time which trunk tip it advertised. Rule:

- **A push pack depends on the trunk ref version current when the push
  commits**, plus, for a scoped-URL push, the version of the scoped ref that
  the client named as `old`, which the compare-and-swap verifies.
- This is sound because trunk is fast-forward-only by default: whatever tip
  the client saw is an ancestor of the current one (26).
- If a repository allows trunk force pushes, a rewind between GET and POST
  leaves the pack depending on objects that are no longer in trunk (27:
  2 missing objects). B1 must then check the pack's external references
  (thin delta bases, plus objects referenced from trees and commits that the
  pack lacks) against the dependency and reject with "trunk was rewritten
  during your push; retry". Stock `git receive-pack` runs an equivalent
  connectivity check on every push. Aspen needs one anyway to stop a client
  from referencing objects that do not exist. What it reads and what it
  costs are in [For N1](#for-n1).
- Over SSH the advertisement and the push share one connection, so the
  server can record exactly what it advertised. C3 can use the exact
  dependency there.

### 4. Force

`git push --force` cannot be seen by the server for unadvertised branches.
v1 requires an explicit signal: `git push -o force` (all refs in the push),
`-o force=<ref>`, or a matching lease. Agents can make it the default with
`git config push.pushOption force`, though that defeats the safety
net and should be documented as such. Trunk force is off by default.

### 5. Delete and lease

- Delete: the short form fails in the client with an error Aspen cannot
  change. Document three routes: the branch-scoped URL with `--delete
  <branch>` (a compare-and-swap delete, 24),
  `git push -o lease=refs/heads/<branch>:<oid> origin --delete
  refs/heads/<branch>` (also compare-and-swap, 09), and
  `git push -o force origin --delete refs/heads/<branch>` (unconditional,
  needs `allow_force`, 09). Without one of these, a full-name delete is
  rejected with a message naming them.
- Lease: two supported routes, both working with every stock client tested:
  - `git push -o lease=refs/heads/x:<oid> origin x` (12, 13).
  - A **branch-scoped push URL**, `https://host/<repo>.git/-/refs/heads/x`,
    which advertises trunk plus exactly that ref. Stock
    `--force-with-lease`, `--delete x` and the usual non-fast-forward hints
    then work (24). The spike accepted a prefix; v1 should accept one exact
    ref name so the dependency stays a single known ref version.
- `explicit-haves` (OpenAI's fork, not upstream as of 2.56.0): the client
  announces it in `Git-Protocol` at discovery, and the server may then
  advertise any refs as named refs while only `.have` lines are pack
  prerequisites. With it, plain `--force-with-lease` and `--delete x` work
  and the pack is still built against trunk alone (25). **Deferred past v1**:
  it needs a fork, and advertising every ref brings back the O(refs)
  advertisement. Revisit if it lands upstream. If implemented, the server
  must verify the client requested the capability in its command list before
  accepting the pack.

### 6. Thin packs

Stock clients send thin packs: deltas against trunk objects the pack does
not contain (29). Advertising `no-thin` avoids them at the cost of sending
every edited file whole (806 B vs 413 B here; far more for large files).

Decision, settled together with spike S3
([../s3/README.md](../s3/README.md#completing-a-thin-push)): **thin packs are accepted on the
wire and completed at ingest** (the missing bases are appended, the object
count rewritten and the trailer recomputed, as `index-pack --fix-thin`
does). `no-thin` stays an optional per-repository setting. S3 showed that
stock clients reject a thin pack served as a packfile URI on every version,
because each downloaded pack is indexed on its own. Completing at ingest
makes every stored pack self-contained, so fetch can never hand a client a
pack whose delta bases it neither has nor receives in the same response,
whether the pack is served as a URI or as a stitched segment.

### 7. Costs accepted

- Every push of a branch resends all its commits since trunk, and stacked
  branches resend their base (21 vs 22). This is Pando's stated trade-off;
  their median push pack was under 20 KiB and p99 under 3 MiB.
- A re-push of an unchanged branch uploads a pack the server discards.
- `git push --tags` sends one command per local tag every time; unchanged
  tags are no-ops.

## For N1

What the core contracts and schema v1 must carry from this spike.

1. **`ref_versions.deps`**: the trunk ref version current when the push
   commits, plus, for a scoped-URL push, the scoped ref's old version.
2. **Two invariants**, stated on the types and checked by B1:
   - every stored pack is *self-contained*: no delta base outside the pack
     (thin pushes completed at ingest, decision 6; compaction writes
     self-contained packs);
   - every committed ref version is *connected*: every object reachable
     from its tip is in its pack or its `deps` closure.
3. **Commit-check verdict** as a value: `create`, `fast_forward`, `forced`,
   `delete_cas`, `delete_unconditional`, `noop`, or `reject { reason,
   retryable }`. A verdict records the exact `ref_version` it was computed
   against, and the commit transaction's compare-and-swap expects that
   version (decision 2). Retryable rejections: ancestry not decidable yet
   (S5's unindexed-commit rule), compare-and-swap lost after the bounded
   recompute, trunk rewritten under the push.
4. **Repository settings**: `allow_force` (`none`, `branches` (default),
   `all`); `no_thin` (default off); writable ref namespaces (default
   `refs/heads/*`, `refs/tags/*`); number of `.have` trunk tips (default 8).
5. **Connectivity check: what it reads and what it costs.** Use the
   object lookup S3 defines for thin-pack completion
   ([../s3/README.md](../s3/README.md#base-object-lookup)): given a ref version
   and an oid, return `(pack_id, offset)` or not found, by probing the
   `.idx` of that version's covering packs. Do not add a second one.
   Search **every ref version advertised on the connection**: trunk (its
   `.have` tips are trunk ancestors) and, for a branch-scoped URL, that
   branch. The client built its pack against both, so thin bases and
   referenced objects may live only in the branch's packs. These versions
   are exactly the push's `deps` (item 1).
   - *Which objects.* Only objects the incoming pack references but does
     not contain: each commit's tree and parents, each tree entry except
     gitlinks (mode `160000`, as Git does), each tag's target. Call this
     set M, deduplicated. Thin delta bases are resolved by the same lookup
     during completion. Nothing reachable from an object in M is walked:
     by the connectivity invariant, an object found in a connected ref
     version brings its whole closure with it. This is the same set
     `git receive-pack`'s connectivity check ends up verifying, minus its
     walk through objects that are already present.
   - *What the lookup reads.* The pack indexes (`.idx`: fanout table plus
     sorted oids) of the covering packs of each searched ref version: its
     own pack and, transitively, its `deps`. After compaction that is the
     cover set the planner would pick for a full fetch of that version
     (`covers`): O(levels) cover packs plus the uncompacted L0 tail. Call
     the total pack count K (at most two versions' cover sets, which
     mostly overlap because a branch depends on trunk). `commit_index` does not
     help here: it maps commits only, and most of M is trees and blobs.
   - *Cost per push.* At most |M| × K index probes, each a fanout lookup
     plus a binary search, O(log N) for N objects in that pack. A found
     object stops at its first hit; only a missing object (the push is
     then rejected) probes all K. Sorting M and merging it against each
     index makes the work sequential. |M| is bounded by the pack's own
     reference count, which is linear in the uncompressed size of its
     trees and commits. So the check costs O(pack tree size × K × log N)
     probes, and the per-push size and object quotas (B3) cap it. Index
     files are immutable and cached (the same cache S3's lookup uses), so
     the steady state reads no object storage.
   - *Contract shape.* One batched call that takes the dependency and a
     sorted oid list and returns, per oid, `(pack, offset)` or absent.
     Completion uses the locations; the connectivity check uses only
     presence.

## Not covered (gaps)

- Clients other than C Git: JGit, libgit2, go-git, gitoxide, Apple Git.
  They belong in the nightly interop matrix (A6). libgit2 and go-git may not
  send push options, which would leave them without the force and lease
  routes; the scoped URL still works for them in principle.
- SSH transport and protocol v1. The commands are the same; SSH removes the
  GET/POST race in decision 3.
- Concurrent pushes to the same ref. The spike serialised commit checks with
  a lock. The verdict-versus-compare-and-swap rule (decision 2) must be
  tested by A3 (compare-and-swap) and B1 (recompute or retryable `ng`) with
  two concurrent pushes to one branch, and by the differential suite and
  the chaos suite's linearizability checker.
- The connectivity check's cost bound (For N1, item 5) is derived, not
  measured.
- The fast-forward check in decision 2 and the dependency check in
  decision 3 were checked with Git as an oracle, not implemented against
  `commit_index`.
- Push certificates (`push-cert`) are not advertised and were not tested.
