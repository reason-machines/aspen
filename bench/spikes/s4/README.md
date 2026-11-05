# Spike S4: streaming `gix-pack` ingest of a multi-GB pack

**Question** (from [`docs/PLAN.md`](../../../docs/PLAN.md), section 3, node S4):
how fast is push ingest with `gix-pack`, and how much memory does it need, for
a multi-GB pack? Spike S3 widened the question. Stored packs must be
self-contained, so ingest also has to complete thin packs. S4 therefore also
measures that cost: the spool, the base lookup, and the second
hash-and-upload pass (push path step 2).

**Answer, in one paragraph.** `gix-pack` 0.77 handles a 6.55 GB, 12-million-object
pack correctly. Its `.idx` is byte-identical to `git index-pack`'s, and it
uses 2.5x less CPU than Git: 270 CPU-seconds against 680. The receive itself
streams in under 10 MiB of memory at any pack size, about 110 MB/s per core.
Turning the pack into object IDs and an `.idx` cannot stream. It needs the
whole pack on local disk (mmapped) and about 140 bytes of heap per object:
1.6 GiB for Linux. Thin-pack completion is cheap in CPU, and the hash pass the
plan feared runs at about 1 GB/s. Two costs need design attention:

- **Base lookups.** gix looks bases up one at a time, inside the receive
  stream. A 100-commit push reads 5,431 pack entries for its bases. If each
  read were a blob-store round trip, the push would take minutes.
- **Stored size.** Completion makes small pushes much bigger. A one-commit
  push grows from 320 B to 37 kB (Git's `--fix-thin` makes 31 kB), and a
  100-commit push grows from 10.5 MB to 57 MB.
- **Duplicate objects.** gix's completion can store an object twice when a
  pack carries it plus a REF_DELTA against it and the server already has it.
  [Section 5](#5-duplicate-objects-in-completion) reproduces this and tests
  the fix the plan adopts.

Everything here was measured on one shared, 2-vCPU cloud machine. See
[Limits of these numbers](#limits-of-these-numbers) before quoting wall times.

## Results

All numbers come from [`results/*.jsonl`](results/); the tables are generated
into [`results/summary.md`](results/summary.md) by `summarize.py`.

- **Linux:** `torvalds/linux` at `6628e7d4c6b2`, cloned with
  `git clone --bare --no-tags`. That gives one pack of 6,551,202,328 bytes
  (6.1 GiB) with 12,004,534 objects. Its largest inflated entry is 24 MB.
- **git.git:** `git/git` at `ebdc087dc99d` gives 324.5 MB and 421,981 objects.
- **Thin pushes:** made from the Linux repository exactly as `git push`
  makes them:
  `printf 'HEAD\n^HEAD~N\n' | git pack-objects --revs --thin --delta-base-offset --stdout`.

### 1. Receive pass: streaming, bounded memory

`stream` parses the pack from a pipe. It reads 64 KiB at a time with no
seeking, inflates every entry to find where the next one starts, computes
each entry's CRC32, and checks the client's trailing SHA-1 with collision
detection (`sha1dc`, the same algorithm Git uses). `stream+spool` also copies
every byte to a file; the copy is checked with `cmp` against the input.

| Pack | CPU s | MB per CPU-s | Peak memory (whole process) |
|---|---:|---:|---:|
| Linux, 6.55 GB | 60.3 (64.1 with spool) | 109 (102) | 8.5 MiB (9.3 MiB) |
| git.git, 325 MB | 2.8 (3.0) | 116 (109) | 7.2 MiB (8.0 MiB) |

The pass is single-threaded, and its memory does not grow with the pack. On
this hardware one push receives at about 0.9 Gbit/s at best, however fast
the network is.

### 2. Full ingest: object IDs and `.idx`

`gix-index` runs `gix_pack::Bundle::write_to_directory`. First it does pass 1
(above) while it spools the pack and builds a delta tree. Then it mmaps the
spool, resolves every delta in parallel to compute each object ID, and writes
a v2 `.idx`. Each object ID is a SHA-1 with collision detection, so this is
where object-level collision detection happens, not in pass 1.
`git-index-pack` is what `receive-pack` runs: `git index-pack --stdin` into an
empty bare repository.

| Linux, 6.55 GB | Wall s | CPU s | Heap MiB | Mapped pack MiB |
|---|---:|---:|---:|---:|
| gix, 2 threads | 205 | 270 | 1,616 | 6,253 |
| gix, 1 thread | 470\* | 276 | 1,558 | 6,253 |
| gix, completion path on (see 3) | 236 | 282 | 1,580 | 6,255 |
| `git index-pack --threads=2` | 591\* | 680 | 1,015 | 7 |

\* Measured while other tenants kept the load average at 3.6–4.6 on 2 vCPUs.
CPU seconds and memory stay steady under load; wall time does not.

- **Correctness:** gix's `.idx` is byte-identical to Git's for both packs
  (`cmp` in `run.sh`).
- **Heap:** about 140 bytes per object (1.6 GiB for 12.0 M objects; Git
  needs 89). Size push limits by object count. The count is in the pack's
  first 12 bytes, so an oversized push can be refused before its body is read.
- **Mapped pack:** the pack is read through `mmap`. Those pages are page
  cache that the kernel can drop and reread. They count in RSS but are not
  heap.
- **Scaling:** pass 1 is serial. On Linux it took 75 s of the 205 s wall
  time with 2 threads. Delta resolution is parallel, and its CPU time barely
  changes with the thread count. Speedup beyond 2 cores was not measured.
- **Spool location:** the spool must sit on a real disk. The first attempt
  spooled to `/tmp`, a tmpfs (RAM): the mmapped pack then counted as shared
  memory, at 370 MiB for a 325 MB pack. `run.sh` refuses a tmpfs work
  directory.

### 3. Thin-pack completion

`thin` is `gix-index` with a base lookup. Here the lookup is the Linux pack,
standing in for the trunk version's covering packs. gix inserts each missing
base, deflated at level 1, just before the first delta that needs it, and
turns that REF_DELTA into an OFS_DELTA. When the stream ends, gix rewrites
the header count and **re-reads the whole spooled pack to compute the new
trailer**, then resolves deltas and writes the `.idx`. `git --fix-thin` is
`git index-pack --stdin --fix-thin` into a repository whose alternates point
at the Linux repository, which is how `receive-pack` runs it. For each
completed pack, `thin.sh` checks that stock Git indexes it in an empty
repository, with no alternates and no `--fix-thin`, and that Git's `.idx` is
byte-identical to gix's. All four passed.

| Push (first-parent commits) | Received | Bases added | Entry reads for bases (p50/p99/max per base) | Stored, gix | Stored, `git --fix-thin` | gix wall / CPU s | gix heap MiB | git wall / CPU s | git heap MiB |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | 320 B | 2 | 4 (2/2/2) | 37.0 kB (116x) | 30.9 kB (96x) | 0.06 / 0.0 | 2 | 0.02 / 0.0 | 0 |
| 100 | 10.5 MB | 4,465 | 5,431 (1/3/9) | 56.8 MB (5.4x) | 48.4 MB (4.6x) | 1.8 / 2.0 | 19 | 6.5 / 6.2 | 167 |
| 3,000 | 319.5 MB | 37,690 | 47,414 (1/3/10) | 576.0 MB (1.8x) | 529.7 MB (1.7x) | 34.5 / 36.8 | 176 | 88.3 / 93.8 | 528 |
| 30,000 | 4.36 GB | 40,675 | 83,676 (2/7/28) | 4.59 GB (1.1x) | 4.55 GB (1.0x) | 173 / 241 | 977 | 563 / 669 | 1,086 |

What this shows:

- **The second pass is cheap.** A sequential SHA-1 over the completed pack
  runs at about 1 GB/s: 4.4 s for 4.59 GB, or 6.8 s if every byte is also
  written to disk (standing in for the upload). Completion's CPU cost is in
  delta resolution, which a non-thin push pays anyway.
- **Every push can take the completion path.** A non-thin pack sent through
  it (`gix-thin-path`) costs 4% more CPU than plain indexing on Linux and
  comes out byte-identical. No separate thin and non-thin code paths are
  needed.
- **Stored size grows, most for small pushes.** Each missing base goes in
  whole. For a one-line change, that means the whole changed file, and the
  root and directory trees down to it. Git's `--fix-thin` grows the pack the
  same way; gix's packs are 1–20% bigger (the gap shrinks as pushes grow),
  because it deflates the bases it inserts at level 1.
  Fetches of that ref version serve the extra bytes too, until compaction
  repacks them.
- **Base lookups are many, and gix makes them one at a time.** Each lookup
  reads the base and its delta chain: 1–2 pack entries typically, at most 28
  here. Each entry is a separate range read when packs live in a blob store.
  gix calls the lookup synchronously, from inside the stream it is parsing.
  Locally the lookups took 0.3–2 s in total. Done serially against a blob
  store at 20 ms per read, the 100-commit push would wait about 110 s for
  its 5,431 reads, and the 3,000-commit push about 16 minutes. These two
  figures are arithmetic, not measurements; B1 measures against a real bucket.

### 4. Commit extraction

`commits` traverses an indexed pack a second time
(`index::File::traverse`). It records each commit's ID, tree and parents,
the rows `commit_index` needs. The output matched
`git log --all --format='%H %T %P'` exactly: 1,505,200 commits for Linux and
85,972 for git.git. On Linux this took 93 CPU-seconds, about a third of
indexing, with 1.9 GiB heap. gix 0.77 offers no hook into the decoded objects
during `write_to_directory`, so extracting commits costs this second pass.

### 5. Duplicate objects in completion

gix's `LookupRefDeltaObjectsIter` asks the external lookup about every
REF_DELTA base it has not inserted itself. It never checks whether the pack
already contains that base. So if a pack carries object X and a REF_DELTA
against X, and the lookup (the push's `deps`) also has X, completion inserts
a second copy of X before the delta. That breaks the "no object twice" rule
for stored packs (spike S1).

[`dup.sh`](dup.sh) builds the case with stock Git: a 30-commit history of one
growing file, a base repository at commit 20 standing in for `deps`, and
packs written by `pack-objects` without `--delta-base-offset`, which makes
every delta a REF_DELTA, as libgit2 does. Each pack goes through `thin`,
with and without `--dedupe`. `--dedupe` is the rule the plan adopts: after
indexing, scan the sorted `.idx` for an ID that appears twice, and if one
does, run completion again with a lookup that answers "not found" for those
IDs, so their deltas resolve against the copy in the pack.

| Pack sent | Mode | Objects in | Entries stored | Repeated IDs | Candidates found in `deps` / not | `git index-pack` | `--strict` |
|---|---|---:|---:|---:|---|---|---|
| Full-history resend, all REF_DELTA | plain | 90 | 92 | 2 | 2 / 13 | accepts | rejects: "appears twice in the pack" |
| Full-history resend, all REF_DELTA | `--dedupe` | 90 | 90 | 0 | 2 / 13; rerun: 0 / 13, plus 15 refused | accepts | accepts |
| 10-commit thin push, all REF_DELTA | either | 30 | 31 | 0 | 1 / 9 | accepts | fails on connectivity only* |
| Full-history resend, OFS_DELTA (stock git) | either | 90 | 90 | 0 | no REF_DELTA | accepts | accepts |

\* `--strict` also checks that every referenced object is present. A
completed thin push is self-contained for deltas, not closed under
reachability: its parent commits live in `deps`. So it fails `--strict` for
that reason alone.

What this shows:

- **The hazard is real** with gix-pack 0.77: a resend written all-REF_DELTA
  stores 2 objects twice, and plain `git index-pack` does not notice.
- **The fix works.** The rerun stores each object once, and `--strict`
  accepts the pack. On these small packs the scan and the rerun take
  well under 0.01 s. On a large pack the rerun repeats the whole completion
  (282 CPU-s for Linux), which is acceptable only because it is rare.
- **Candidate bases are not thin bases.** In the ordinary all-REF_DELTA push,
  9 of 10 REF_DELTA bases are in the pack and only 1 in `deps`. A lookup
  miss is expected and must not fail the push.

Not covered here: a REF_DELTA before its own in-pack base (S1 found no client
that sends one), and a client resend at Linux scale.

## What N1, A2 and B1 should take from this

1. **Ingest pipeline (A2), in this order:**
   1. Receive into a local spool on a real disk, with the streaming parse
      (pass 1). Refuse the push early from the header's object count.
   2. Collect the base IDs of all REF_DELTA entries as *candidate* bases.
      Stock `git push` sends only OFS_DELTA inside the pack, so for it the
      candidates are exactly the thin bases, but libgit2 sends every delta as
      REF_DELTA, mostly against bases in the same pack (S1), and pass 1 can't
      tell which is which.
   3. Prefetch the candidates and their delta chains **in parallel**, from
      the `.idx`-probed covering packs (S3's lookup), into memory or a local
      file. A candidate that isn't found is expected to be in the pack, not
      an error. A chain takes p99 3–7 dependent reads, so the prefetch needs
      a few rounds of parallel reads, not thousands of serial ones. This
      step is designed, not measured: the harness's lookup is synchronous
      over a local pack.
   4. Run `Bundle::write_to_directory` over the spool with a lookup that
      answers from the prefetched set. That one call completes the pack,
      re-hashes it, resolves deltas and writes the `.idx`. A base in
      neither the prefetched set nor the pack fails it.
   5. Scan the sorted `.idx` for a repeated ID; if there is one, rerun step
      4 with the lookup refusing those IDs (section 5).
   6. Repair forward bases (a REF_DELTA before its in-pack base, which gix
      stores as it came), as the plan's push step 2 describes (S1).
   7. Upload the pack and the `.idx` with one sequential read each (about
      1 GB/s; the page cache is still warm).

   The CPU cost per push is pass 1 plus step 4, which parses the spool a
   second time: for Linux, 64 + 282, about 346 CPU-seconds, against 680 for
   `git index-pack`. The scan in step 5 is one pass over the `.idx`.
   Prefetch and upload I/O come on top. Never pass a lookup that does
   blob-store reads to gix while it reads the network stream.
2. **Contracts (N1):**
   - The base lookup gix calls is the synchronous `gix_object::Find`. Any
     `aspen-core` trait wrapped around it has to answer from local data.
     Network I/O belongs in a separate, async prefetch step.
   - The blob store needs only "upload this local file (multipart)", plus a
     ranged read for base prefetch. No streaming tee into the blob store
     during the receive.
   - `packs` should record the `.idx` location and size beside the pack. The
     `.idx` is about 6% of the pack: 417 MB for Linux.
   - Record received bytes as well as stored bytes, so the completion growth
     is visible in metrics.
3. **Limits and sizing (B1, B3):**
   - Heap of about 140 B per object, plus the largest object, plus delta
     buffers. Set gix's `alloc_limit_bytes`.
   - Local disk of about 2x the largest accepted push (spool plus completed
     pack) plus its `.idx`.
   - Push size limits checked against the header's object count before the
     body is read.
4. **`commit_index` (A2, A3):** the tip commit needs one `.idx` lookup in the
   new pack plus one object decode. Every other commit comes from the second
   traversal, which belongs in the background job the plan already has
   (93 CPU-s for all of Linux). A gix hook that hands decoded objects to the
   caller during indexing would save that pass; it would be a change upstream
   in gitoxide.
5. **Pin gix exactly.** The `gix-*` crates are pre-1.0. A2 should pin exact
   versions, as this spike does (`gix-pack = "=0.77.0"`), and upgrade on
   purpose.

## Limits of these numbers

- **Shared machine.** One 2-vCPU cloud machine (Intel Xeon, 16 GiB) shared
  with other Sessions; the load average was 1.2–5.4 during the runs and is
  recorded with every result. CPU seconds and memory are reliable. Wall
  times are upper bounds, and single-thread versus two-thread comparisons
  are unreliable. Scaling past 2 cores was not measured.
- **Local base lookups.** Lookups read a local pack. Blob-store latency is
  arithmetic from the measured read counts, not a measurement.
- **No memory limit tested.** Indexing under a container memory limit
  smaller than the mapped pack was not run. Mapped pages can be dropped in
  principle, at some cost in throughput.
- **Packs from stock Git only.** Packs from JGit or libgit2 were not
  measured at scale; section 5 only imitates libgit2's all-REF_DELTA packs
  with small packs from stock Git. Objects over 24 MB, and hostile packs (delta cycles, zip bombs)
  were not tested. Hostile input is fuzzing work (D3).
- **Not a benchmark of the whole push.** No HTTP, no Postgres and no real
  upload took part. B1 measures push latency end to end.

## Reproduce

Requirements: Linux, Rust 1.99.1, `git`, `jq` and `python3`. The work
directory needs real disk with room for about 3 copies of the largest pack.
This spike is a standalone Cargo project, outside the main workspace and its
CI.

```bash
git clone --bare --no-tags https://github.com/torvalds/linux.git /data/linux.git   # ~15 min, 6.1 GiB

cd bench/spikes/s4
cargo build --release --locked

# Full ingest of one pack. Pick stages and thread counts; each stage appends
# to results/<repo>.jsonl.
STAGES="stream git gix thinpath commits" ./run.sh /data/linux.git/objects/pack/*.pack /data/work 1 2

# Thin pushes as git push makes them, completed against the same repository.
for n in 1 100 3000 30000; do
  printf 'HEAD\n^HEAD~%s\n' "$n" |
    git --git-dir=/data/linux.git pack-objects --revs --thin --delta-base-offset --stdout >/data/push-$n.pack
  ./thin.sh /data/linux.git /data/push-$n.pack /data/work 2
done

./summarize.py   # rewrites results/summary.md

# Duplicate-object hazard (section 5): small, self-contained, about 5 s.
./dup.sh /tmp/s4-dup   # rewrites results/dup.jsonl
```

Files:

- [`src/main.rs`](src/main.rs): the harness, with the stages `stream`,
  `index`, `thin` (with `--dedupe`), `commits` and `rehash`. Each prints one JSON object, with
  memory sampled from `/proc/self/status`.
- [`measure.py`](measure.py): wall time, CPU time and peak memory of a whole
  process tree, so `cat | git index-pack` and the harness are measured the
  same way.
- [`run.sh`](run.sh) and [`thin.sh`](thin.sh): the experiments, including
  the correctness checks (`cmp` of the spool, the `.idx` and the commit
  graph, and Git accepting each completed pack).
- [`dup.sh`](dup.sh): the duplicate-object repro and its fix (section 5).
- [`summarize.py`](summarize.py): writes the tables in `results/summary.md`.
