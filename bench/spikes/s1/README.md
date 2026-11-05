# Spike S1: stitched packs across Git clients

**Question.** Aspen serves a fetch from clients that don't use packfile URIs
by *stitching*: it concatenates the entries of the stored packs the planner
selected, rewrites the header object count and recomputes the trailing SHA-1,
without inflating or recompressing anything (docs/PLAN.md, fetch path
step 4). Do stock clients accept such a pack? What happens when segments
overlap (the same object in two packs) or when delta bases live in another
segment, after the delta, or only in the client's repository (thin)?

**Answer in one paragraph.** Plain stitching works on every tested client
*only if no object id appears twice*. As soon as two segments share an
object, libgit2 rejects the pack, git 2.30 through 2.47 reject it whenever a
duplicated object is a REF_DELTA base, every git version rejects it with
`fetch.fsckObjects` or `transfer.fsckObjects`, and go-git reports success but
writes a pack that stock git cannot read. Duplicates are not an edge case for
Aspen: because every stored push pack is self-contained (S3), the bases that
completion appends are copies of objects in older packs, so even a clone from
three consecutive pushes contains duplicates (`sc-clone`). The fix is a
stitcher that drops later copies of an object and rewrites the OFS_DELTA
headers that pointed at a dropped copy. It is still a header-only byte
operation: entry data is copied unchanged. With that stitcher, every scenario
passes on all 48 client configurations. go-git also never asks for
`thin-pack`, and it rejects deltas whose base comes later in the pack, so a
stitched pack must put every base before its deltas; if stored packs are
written base-first, keep-first deduplication gives that order in one
streaming pass.

The full matrix is in [MATRIX.md](MATRIX.md) (1,008 runs). The rest of this
file is what N1 (core contracts and schema v1), A2 (`aspen-pack`, the
stitcher) and B2 (upload-pack) need from it.

## What was tested

Clients (48 configurations):

| Client | Versions | Configurations |
|---|---|---|
| git | 2.30.9, 2.39.5, 2.43.7, 2.47.3, 2.56.0 (latest) | protocol v0 and v2; received through `index-pack` (clones and fetches of 100 objects or more) or `unpack-objects` (smaller fetches, the default `fetch.unpackLimit`); `fetch.fsckObjects` off and on |
| JGit | 7.8.0.202609011348-r | protocol v0 and v2, `fetch.fsckObjects` off and on |
| gitoxide | `gix` 0.90.0 | protocol v1 and v2 |
| libgit2 | 1.9.7 (`git2` 0.21.0, vendored) | its default (v0) |
| go-git | v5.19.3 | its default (v0) |

Each client fetches `refs/heads/main` from a deliberately dumb upload-pack
([fake_server.py](fake_server.py)) that ignores negotiation and always sends
one pre-built pack. Git runs it over a pipe (`--upload-pack`); the libraries
use its smart-HTTP endpoint. The result is then verified with git 2.56:
`refs/remotes/origin/main` must be at the expected tip, `git fsck --strict
--full` must pass, and the expected number of objects must be reachable.
A cell is `ok`, `REJECT` (client failed), `CORRUPT` (client said yes, the
repository is broken) or `TIMEOUT`.

Segments are cut from a six-commit history the way a server would hold
them ([make_scenarios.py](make_scenarios.py)):

| Pack | Contents | Like |
|---|---|---|
| A | C1..C3, full | the first push |
| B, C | C4..C5 and C6, thin (REF_DELTA bases in older packs) | later pushes as received |
| Bsc, Csc | B and C completed with `index-pack --fix-thin` | later pushes as Aspen stores them after S3 |
| AB, FULL | C1..C5 and C1..C6, full | compacted covers |

The 21 scenarios cover a single pack, a negative control (wrong header count,
which every client rejected, as it must), multi-segment clones, thin
incremental fetches, forward references (a REF_DELTA before its base),
over-fetch of objects the client has, three kinds of duplicates (overlapping
covers, the same segment twice, completed bases), and each of those through
the stitcher this spike recommends. The scenario table in MATRIX.md lists
the measured properties of each pack (duplicates, cross-segment, forward and
thin REF_DELTAs), so every claim below is backed by a scenario that actually
contains the case.

## Findings

### 1. Plain concatenation is accepted when no object repeats

`clone-3seg` (A+B+C, 21 REF_DELTAs whose base is in an earlier segment),
`thin-2seg` (18 bases only in the client's repository) and `overfetch-have`
(every object of A and B already in the client) are accepted by all 48
configurations. Offsets stay valid inside each segment, as the plan assumed,
and no client cares where a segment boundary is.

### 2. Duplicate objects break most clients

| Client | Duplicates in a stitched pack (`dup-overlap`, `dup-exact`, `dup-thin`, `sc-clone`, `sc-fetch`) |
|---|---|
| git 2.30–2.47, `index-pack` | **Rejected** when a duplicated object is a REF_DELTA base: `REF_DELTA at offset N already resolved (duplicate base X?)`. Accepted otherwise (`dup-thin`), with the duplicate written into the `.idx`. The check was removed in git 2.50.0 (source inspection of 2.48.0–2.52.0 tarballs; 2.50 was not run) |
| git 2.56, `index-pack` | Accepted; duplicates are written into the `.idx` |
| git, any version, `unpack-objects` | Accepted (the second copy is silently ignored) |
| git, any version, `fetch.fsckObjects` / `transfer.fsckObjects` | **Rejected**: `The same object X appears twice in the pack` (index-pack `--strict`) |
| libgit2 1.9.7 | **Rejected**: `duplicate object X found in pack`. On `sc-fetch` it ran out of memory instead (4 GiB address-space cap) |
| go-git v5.19.3 | **Corrupt**: reports success, but writes an `.idx` without the duplicates, and stock git then refuses the pack (`claims to have 118 objects while index indicates 73`) |
| JGit 7.8, gitoxide 0.90 | Accepted, duplicates in the `.idx`; fsck passes |

So a stitched pack must never contain an object id twice. Since every stored
push pack is self-contained (S3), its completed bases are copies of objects
that older packs also hold: `sc-clone`, a plain clone from three stored push
packs, has 19 duplicates out of 92 entries and is rejected by libgit2, by git
2.30–2.47 and by everyone with fsck on.

### 3. Deduplicating naively creates delta cycles

The obvious fix, keeping the first copy of each object and turning OFS_DELTA
into REF_DELTA wherever a segment lost an entry (`dup-thin+naive`), can
produce two objects that are each a delta on the other. Both copies were
valid in their own packs; with the first copy of X taken from one pack and
the first copy of Y from the other, the kept entries point at each other.
git 2.56 and JGit accept that pack, and the client's `git fsck` then exhausts
memory (it reached 27 GB before this harness capped clients at 4 GiB).
libgit2 also exhausts memory; git ≤ 2.47 `index-pack` and go-git reject
it. `unpack-objects` (small fetches) stores correct loose objects. A stitcher
bug of this kind turns into a client-side denial of service, so the rule
below must be enforced, not just intended. The function is kept in
[packlib.py](packlib.py) as `stitch_naive_dedupe` with its failure documented.

### 4. go-git needs non-thin packs with bases first

go-git never sends `thin-pack` (see the capabilities table in MATRIX.md), so
a thin pack is a protocol violation for it, and it fails with `reference
delta not found` on `thin-2seg`. It also fails on `reverse-order`, where
REF_DELTA bases come after the deltas that need them (21 forward
references). Every other client accepts both. Stored packs are
self-contained, so stitched packs are never thin; the ordering rule still
matters, for overlapping covers and for segments listed out of order.

Completed push packs contain forward references inside one segment: like
`index-pack --fix-thin`, completion appends the missing bases *after* the
received entries (14 of 30 entries in Bsc). On its own such a pack is fine
for every client (`sc-single`), but it forces the stitcher to reorder once
duplicates are dropped.

### 5. The fix: deduplicate, bases before deltas, rewrite headers only

[`packlib.stitch_topo`](packlib.py) emits segment entries in order and:

- drops every copy of an object id after the first one emitted;
- emits a delta only after some copy of its base has been emitted (or, for a
  client that asked for `thin-pack`, when the base is in no segment);
- recomputes the OFS_DELTA distance against wherever the base was emitted,
  and copies REF_DELTA entries unchanged;
- copies every zlib stream byte for byte. Only entry headers change, and the
  output is byte-identical to plain stitching when there is nothing to fix.

It never makes a cycle, because a delta waits until its base is in the
output. Every `+topo` scenario passes on all 48 configurations, and so does
`thin-2seg+bases`, where external bases are sent whole for a client that
didn't ask for `thin-pack`. [check_topo.py](check_topo.py) adds 500 random
stitch plans (1–5 segments from all seven packs, repeats and any order; 380
with duplicates, 116 with forward references), each accepted by
`index-pack --strict --fix-thin` on all five git versions and checked for
no duplicate ids, no REF_DELTA before its base, no external base in a
non-thin pack, no hidden cycle and the exact expected object set.

**One pass is enough if stored packs are base-first.** If every stored pack
is self-contained *and* has each base before the deltas that use it, the
keep-first rule never has to hold an entry back. Every kept entry's base
precedes it in its own segment, and that base was either kept or dropped
because an even earlier copy was already emitted. `check_topo.py` checks
this on all 155 sequences of 1–3 base-first segments (A, AB, FULL and
base-first Bsc and Csc, any order, with repeats): all are stitched in one
pass with no entry moved. Git's own compacted packs (`pack-objects`) are
already base-first; completed push packs are base-first only if completion
writes the appended bases before the received entries instead of after.

### 6. Clients push base-first already

The received entries of a push are in the client's order, and stock
`receive-pack` accepts a REF_DELTA that comes before its base.
[push_order.py](push_order.py) checks whether any client sends one. Every
client that can push (git 2.30.9–2.56.0, JGit, libgit2, go-git; gitoxide has
no push) pushes a 24-commit history with renames and copies to stock git
2.56 `receive-pack` with `receive.unpackLimit=1`, so the client's bytes are
kept. There are four pushes per client: from a loose or an
`gc --aggressive` source, of the whole history or of the second half on top
of the first (thin). The script then looks in the kept pack for a delta
whose base the client sent later in the same pack.

**None of the 32 pushes has one** ([push_order.tsv](push_order.tsv)). git,
JGit and go-git send OFS_DELTA inside the pack and REF_DELTA only for thin
bases the server already has. libgit2 sends every delta as REF_DELTA, still
base-first. JGit sent the whole history (182 entries) for the incremental
push too, not a thin pack. Aspen still repairs forward bases at ingest
(below), because other clients and future versions may send them.

## For N1, A2 and B2

These are the contracts that follow from the findings. The plan changes in
this PR (docs/PLAN.md fetch path step 4, push path step 2, decisions log)
record the same rules.

1. **Stitched-pack invariants.** A stitched pack served to a client:
   - contains each object id at most once;
   - has every delta's base earlier in the pack, or, only if the client sent
     `thin-pack`, in the client's haves (stored packs are self-contained, so
     Aspen never needs that case);
   - has a header count equal to the number of entries actually emitted.

   Clients that violate nothing still accept extra objects the client
   already has (`overfetch-have`), so over-fetch is safe; duplicates are not.
2. **Stored packs are base-first.** Thin-pack completion (push path step 2)
   writes the appended bases before the received entries, so the spool
   file's second pass emits `header, bases, received entries, trailer`.
   OFS_DELTA distances are relative, so moving every received entry by the
   same amount keeps them valid; only the `.idx` offsets change, and the
   `.idx` is built after the final layout anyway. Compaction output from
   `pack-objects` style writers is already base-first.

   A client's received entries can put a REF_DELTA before its base, and
   stock git accepts that, so ingest **repairs, never rejects** it
   (finding 6: no tested client does this, but nothing forbids it). Pass 1
   resolves every delta, so it knows which entries have forward bases and
   every entry's offset, header length and size. Pass 2 then emits each
   such entry, and anything based on it, just after its base: random reads
   of the local spool file, with every OFS_DELTA whose distance changed
   getting a recomputed header, and compressed data copied unchanged.
   Compaction must write base-first too. A2 should check the property on
   every pack it writes.

   The production stitcher is single-pass. Unlike `packlib.stitch_topo`,
   which keeps going and lets the first copy *to become emittable* win (so
   it silently reorders), it must treat a forward base as a broken `packs`
   invariant: fail the fetch with a retryable `ERR`, and never reorder or
   buffer entries to cover for it. A retry can't heal it, because the
   stored pack is still wrong, so the event is also an alert (an error
   metric that pages), not just a log line. The fix is a compaction or
   repair job that rewrites that pack base-first.
3. **What the stitcher needs per segment.** Each segment's `offset → oid`
   table (the `.idx` has it) and, for each entry, its kind, header length and
   base (offset or oid). The plan must know the output object count before
   the first byte, because the header comes first. N1 should decide whether
   that per-entry table is a sidecar blob written at ingest next to the
   `.idx` (one sequential read per fetch, no range reads into the pack) or
   whether the stitcher range-reads entry headers. The sidecar is the
   simpler, cheaper option.
4. **Memory.** `.idx` files are sorted by object id, so *finding* the
   duplicate set of N segments is a k-way merge of their `.idx` files in
   O(N) memory. *Emitting* costs more. A kept OFS_DELTA whose base copy was
   dropped needs the output offset of the base's first copy, and dropped
   entries and resized OFS_DELTA headers shift every later entry. So the
   stitcher holds every duplicated id with its emitted offset, plus the
   current segment's old-offset to new-offset map: O(duplicates + entries
   in the largest segment). (The reference `stitch_topo` is simpler and
   holds an oid to offset map for every object.) Covers that don't overlap
   reduce duplicates to the completed bases.
5. **Cost.** Dropping entries only shrinks the output. `sc-clone` is
   392,583 bytes plain and 223,383 deduplicated, which is byte-identical to
   stitching the original thin pushes (`clone-3seg`) and 0.3% larger than a
   fresh `pack-objects` of the whole history (222,739). Entries whose OFS_DELTA distance changes grow or shrink by
   a few header bytes. Nothing is inflated or recompressed, except the
   external bases of a non-thin pack for a client without `thin-pack`
   (`thin-2seg+bases`), which Aspen does not need because stored packs are
   self-contained.
6. **Test oracle for A2 and the fuzzer.** The invariants above are cheap to
   check on output: parse the pack, resolve object ids, assert no repeats and
   no forward base. `check_topo.py` is the reference; A2's property tests
   and the D3 stitcher fuzz target should assert the same invariants, plus
   acceptance by `git index-pack --strict` on git 2.30 (the strictest about
   duplicated bases) and the latest git.
7. **Delta chain depth across segments.** Keep-first can lengthen chains.
   If segment 1 has B as a delta of A and segment 2 has D as a delta of a
   full copy of B, segment 2's B is dropped and D's chain becomes
   D → B → A. Depth can approach the sum of the per-segment depths, so it
   is bounded by the number of segments. A2 should measure the maximum
   chain depth of stitched packs and check that git 2.30 `index-pack`
   accepts the worst case the compaction levels allow.

## Reproducing

```bash
cd bench/spikes/s1
./setup.sh                         # about 6 minutes on 4 cores; builds git 2.30-2.56 from source,
                                   # fetches Go, a JDK and JGit, builds the Rust and Go harnesses
./run_matrix.py                    # about 4 minutes; rewrites MATRIX.md and results.tsv
./check_topo.py --rounds 500       # about 1 minute; randomised check of the stitcher
./push_order.py > push_order.tsv   # about 2 minutes; do clients push forward bases?
```

No root and no Docker are needed: everything is installed under
`$ASPEN_S1_CACHE` (default `~/.cache/aspen-s1`). The build needs a C
compiler, `make`, `curl`, `xz`, Python 3.11 and `cargo`. Git is built
without curl, OpenSSL or expat, because the harness drives it over a pipe;
the transport does not affect how a pack is indexed. Versions are pinned at
the top of [setup.sh](setup.sh) (git through `GIT_VERSIONS`). Everything is
deterministic: fixed content seed, fixed commit dates and single-threaded
`pack-objects`, so pack sizes and counts repeat exactly.

Files:

| File | Role |
|---|---|
| [packlib.py](packlib.py) | Pack v2 parser, `stitch_plain`, `stitch_topo` (recommended), `stitch_naive_dedupe` (negative result) |
| [make_scenarios.py](make_scenarios.py) | Builds the history, the segment packs and the 21 stitched scenario packs |
| [fake_server.py](fake_server.py) | Upload-pack that always sends the scenario's pack: v0/v1/v2, pipe and smart HTTP |
| [run_matrix.py](run_matrix.py) | Runs every client configuration on every scenario and verifies the result |
| [check_topo.py](check_topo.py) | Randomised stitcher check and the single-pass check |
| [push_order.py](push_order.py) | Pushes from every client and looks for forward bases in the received pack |
| `harness-rust/`, `harness-go/`, `harness-java/` | Minimal fetch programs for gitoxide and libgit2, go-git, and JGit; each also has a push mode for `push_order.py` |
| [MATRIX.md](MATRIX.md), [results.tsv](results.tsv), [push_order.tsv](push_order.tsv) | Generated results |

## Limits

- The history is small (73 objects, 223 KB). It contains every structural
  case listed above, but it doesn't measure stitching throughput or memory
  on large packs. That is A2's benchmark, on S4's multi-GB packs.
- Only HTTP (libraries) and a pipe (git) were used. SSH carries the same
  bytes and was not run.
- Apple Git was not run (no macOS host); it is a patched git 2.39–2.50
  depending on the Xcode release, so the git 2.39 and 2.47 rows are the
  closest proxy. The nightly interop matrix (A6) should add it.
- libgit2 was tested through `git2` with a vendored libgit2 1.9.7, the
  version most Rust and many other bindings ship; distribution libgit2
  builds may be older.
- The git ≤ 2.49 versus ≥ 2.50 boundary for duplicated REF_DELTA bases comes
  from reading the 2.48.0–2.52.0 sources, not from running 2.48–2.55.
- go-git v6 is still in beta and was not tested.
- gitoxide appends a second copy of REF_DELTA bases it finds in the local
  repository even when the pack already contains them (`overfetch-have`,
  19 extra entries). That is a gitoxide quirk, not caused by stitching, and
  fsck passes.
