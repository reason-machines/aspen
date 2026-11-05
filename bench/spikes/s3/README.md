# Spike S3: `packfile-uri` across client versions

**Question.** Aspen wants to serve fetches the way Pando does: answer the
protocol v2 `fetch` with an empty inline pack plus `packfile-uris` that point
at stored packs in object storage or a CDN. Which git clients accept that,
what must the stored packs and the blob server look like, and what happens
when a download is interrupted and resumed?

**Answer in one paragraph.** Every stock git client from 2.30 to 2.56
handles packfile URIs when the user opts in with `fetch.uriprotocols`, and
every one of them resumes an interrupted download with an HTTP `Range`
request on the next `git fetch`. It works only under strict conditions,
though. Each URI pack must be **self-contained**: a thin pack fails on every
version. The advertised hash must be the pack's trailing SHA-1. The URL
must not redirect, must not need credentials, and must honour `Range`. The
server must always send a `packfile` section. Older clients (2.55 and
earlier) get stuck for good if the blob server answers a resume at
end-of-file with `416`, which is what S3 and GCS do. No client retries a
failed URI download on its own except OpenAI's fork, and an interrupted
`git clone` throws everything away. The non-C clients (JGit, libgit2,
go-git and gitoxide) never ask for packfile URIs, so they always take the
stitched-pack path.

The full table is in [results/matrix.md](results/matrix.md). The rest of
this file is what N1 (core contracts and schema v1) and the B2 fetch service
need from it.

To regenerate the results, build the clients with `build-git.sh` and run
`python3 bench/spikes/s3/matrix.py` (about 3 minutes). It rewrites
`results/matrix.md` and also writes `results/results.json`, the raw
observation for every cell (requests, HTTP headers, client `User-Agent`,
exit codes, stderr). The JSON is generated bulk and is not committed.

---

## What was tested

### Clients

| Name | Version | How |
|---|---|---|
| 2.30 | `git version 2.30.9` | built from source (`build-git.sh 2.30 v2.30.9`) |
| system | `git version 2.39.5` | Debian 12 package |
| 2.43 | `git version 2.43.7` | built from source |
| 2.47 | `git version 2.47.3` | built from source |
| 2.56 | `git version 2.56.0` | built from source; latest upstream release on 2026-11-03 |
| openai | `git version 2.55.0.921.g9fe64f46bd` | OpenAI's fork ([openai/git](https://github.com/openai/git), branch `codex` at `9fe64f46bd`), the client Pando ships |

Non-C clients were checked by reading their source at the commits below,
not by running them. No JVM was available on the spike machine, and
building them was out of proportion for a "does it ever send this
argument" question.

| Client | Commit | Sends `packfile-uris`? |
|---|---|---|
| JGit | `8c24a6aa62` | No. `BasePackFetchConnection` says the section "would have to be handled here" and expects `packfile` next. JGit supports packfile URIs only as a *server*. |
| libgit2 | `42a89d7cc9` | No. libgit2 does not implement protocol v2 at all. |
| go-git | `dba3317730` | No. It can decode the response section, but the client never requests it. |
| gitoxide | `c14938e4bc` | No. `gix-protocol` comments it out: "packfile-uris must be configurable and can't just be used". |

### Harness

* [server.py](server.py) is a small Python smart-HTTP server. It passes
  `ls-refs` and protocol v0 through to the real `git upload-pack`, and
  answers any v2 `fetch` that carries `packfile-uris` itself: it sends
  `ready`, the URI list from the scenario, and an inline pack (empty by
  default). On the same port it runs a blob store that serves packs with
  `Range` support and can misbehave on purpose: truncate, stall, return a
  status code, ignore `Range`, redirect, or demand auth.
* [matrix.py](matrix.py) builds a fixture repository and cuts packs from it
  with `git pack-objects`: full, incremental, thin, REF_DELTA-only, and 50
  one-commit packs. It then runs every scenario against every client, and
  after each successful step checks `origin/main` and runs
  `git fsck --strict`. The blob host is `127.0.0.2` and the Git host is
  `127.0.0.1`, so the client sees them as different origins, like a bucket
  or CDN.
* [build-git.sh](build-git.sh) builds a git version from source.

### Reproduce

```bash
# One-time: a git.git clone with OpenAI's fork as a remote.
git clone --filter=blob:none https://github.com/git/git.git /tmp/gitsrc
git -C /tmp/gitsrc remote add openai https://github.com/openai/git.git
git -C /tmp/gitsrc fetch --filter=blob:none openai

# Build the clients (about 1.5 minutes each on 2 cores). Needs curl, zlib,
# openssl and expat headers. Without root, extract the Debian -dev packages
# (apt-get download libcurl4-openssl-dev zlib1g-dev libssl-dev libexpat1-dev;
# dpkg -x each into /tmp/sysroot) and set SYSROOT=/tmp/sysroot.
for v in "2.30 v2.30.9" "2.43 v2.43.7" "2.47 v2.47.3" "2.56 v2.56.0" \
         "openai 9fe64f46bdef176eabbc4e7f1f15c8e4f8dbc325"; do
  bench/spikes/s3/build-git.sh $v
done

# Run the matrix (about 3 minutes). Standard library Python 3 only.
python3 bench/spikes/s3/matrix.py
# or a subset: --clients 2.30,2.56 --only E1,E5
```

---

## Results

`OK` means the command succeeded, `origin/main` landed on the expected
commit, and `git fsck --strict` passed. `FAIL` means git exited non-zero.
In every `FAIL` row the refs did **not** move, so no client ever ended up
with a ref pointing at missing objects. Exact error text for each cell is in
[results/matrix.md](results/matrix.md).

### A. Opt-in

| # | Scenario | 2.30 | 2.39 | 2.43 | 2.47 | 2.56 | openai |
|---|---|---|---|---|---|---|---|
| A1 | `fetch.uriprotocols` unset | not requested | not requested | not requested | not requested | not requested | not requested |
| A2 | `uriprotocols=http`, protocol v0 | not requested | not requested | not requested | not requested | not requested | not requested |
| A3 | server does not advertise `packfile-uris` | not requested | not requested | not requested | not requested | not requested | not requested |
| A4 | client allows `https` only, server sends an `http` URI | **downloads it** | **downloads it** | **downloads it** | **downloads it** | **downloads it** | **downloads it** |

### B. Happy paths and plan shapes

| # | Scenario | 2.30 | 2.39 | 2.43 | 2.47 | 2.56 | openai |
|---|---|---|---|---|---|---|---|
| B1 | clone: 1 self-contained URI pack + empty inline pack | OK | OK | OK | OK | OK | OK |
| B2 | clone: 3 layered URI packs (base + 2 increments) | OK | OK | OK | OK | OK | OK |
| B3 | fetch with haves: 1 incremental URI pack | OK | OK¹ | OK¹ | OK¹ | OK¹ | OK¹ |
| B4 | clone: URI packs overlap (duplicate objects) | OK | OK | OK | OK | OK | OK |
| B5 | clone: URI pack + non-empty inline pack | OK | OK | OK | OK | OK | OK |
| B6 | `packfile-uris` section with no `packfile` section | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL |
| B7 | clone: 51 URI packs | OK 1.1 s | OK 0.9 s | OK 0.6 s | OK 1.6 s | OK 2.3 s | OK 2.2 s |
| B8 | 51 URI packs, `fetch.packfileUriJobs=8`, `no-ref-delta` advertised | OK 2.2 s | OK 2.5 s | OK 1.3 s | OK 2.8 s | OK 2.6 s | OK **0.6 s** (parallel) |
| B9 | self-contained pack using REF_DELTA, no OFS_DELTA | OK | OK | OK | OK | OK | OK |
| B10 | URI answers `302` | FAIL | FAIL | FAIL | FAIL | FAIL | OK |
| B11 | URI answers `302`, client sets `http.followRedirects=true` | OK | OK | OK | OK | OK | OK |
| B12 | 3 fetches, each with a URI pack + non-empty inline pack | OK | OK¹ | OK¹ | OK¹ | OK¹ | OK¹ |

¹ Succeeds, but leaves a `.keep` on that fetch's **inline** pack (see finding 9).
B7/B8 times are wall-clock on one 2-core machine and only show relative cost.

### C. Deltas across pack boundaries

| # | Scenario | 2.30 | 2.39 | 2.43 | 2.47 | 2.56 | openai |
|---|---|---|---|---|---|---|---|
| C1 | fetch: URI pack is thin against objects the client already has | FAIL² | FAIL³ | FAIL³ | FAIL³ | FAIL³ | FAIL³ |
| C2 | clone: URI #2 is thin against objects in URI #1 | FAIL² | FAIL³ | FAIL³ | FAIL³ | FAIL³ | FAIL³ |
| C3 | clone: thin URI listed before its base pack | FAIL² | FAIL² | FAIL² | FAIL² | FAIL² | FAIL² |
| C5 | as C1, server advertises the hash git 2.39 computes after `--fix-thin` | FAIL² | OK | OK | OK | OK | OK |
| C6 | as C5, client sets `core.loosecompression=9` | FAIL² | FAIL³ | FAIL³ | FAIL³ | FAIL³ | FAIL³ |
| C4 | `jobs=4` + `no-ref-delta` advertised, but a URI pack has REF_DELTA | OK | OK | OK | OK | OK | FAIL⁴ |

² `pack has 4 unresolved deltas`. Before 2.31, `http-fetch` does not pass `--fix-thin`.
³ `pack downloaded from … does not match expected hash`. Since 2.31, `index-pack --fix-thin` appends the missing bases, which changes the pack's trailing hash.
⁴ `REF_DELTA not allowed by --no-ref-delta`. The fork enforces the promise.

### D. Integrity and HTTP failures

| # | Scenario | 2.30 | 2.39 | 2.43 | 2.47 | 2.56 | openai |
|---|---|---|---|---|---|---|---|
| D1 | advertised hash does not match the pack | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL |
| D2 | URI packs miss objects | FAIL, clone dir removed | same | same | same | same | same |
| D3 | URI returns 404 | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL |
| D4 | URI returns 403 (expired presigned URL) | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL |
| D5 | 503 once, then OK | FAIL | FAIL | FAIL | FAIL | FAIL | OK (retried) |
| D6 | 503 + `Retry-After: 1` once, then OK | FAIL | FAIL | FAIL | FAIL | FAIL | OK (retried) |
| D7 | 429 once, then OK (`http.maxRetries=2`) | FAIL | FAIL | FAIL | FAIL | FAIL | OK (retried) |
| D8 | URI needs Basic auth, helper has creds for the blob host | FAIL (401) | FAIL | FAIL | FAIL | FAIL | OK |
| D9 | repo needs auth; are repo creds sent to a same-host blob URL? | not sent | not sent | not sent | not sent | not sent | not sent |

### E. Interrupted downloads and resume

Pack size is 24,297 bytes. Each "retry" is the user running `git fetch`
again: no client resumes inside one command.

| # | Scenario | 2.30 | 2.39 | 2.43 | 2.47 | 2.56 | openai |
|---|---|---|---|---|---|---|---|
| E1 | connection cut at 50%, then retry | resumes (`Range: bytes=12148-`) | resumes | resumes | resumes | resumes | resumes |
| E2 | Ctrl-C at 50%, then retry | resumes from 8192 | resumes | resumes | resumes | resumes | resumes |
| E11 | SIGKILL at 50%, then retry | resumes from 8192 | resumes | resumes | resumes | resumes | resumes |
| E3 | cut at 50%, retry gets a **new presigned URL** for the same pack | resumes | resumes | resumes | resumes | resumes | resumes |
| E4 | cut at 50%, retry server ignores `Range` (200 + full body) | 2nd fails (corrupt), 3rd OK | same | same | same | same | same |
| E5 | complete `.temp` present, server answers `416` | **stuck: fails every time** | **stuck** | **stuck** | **stuck** | OK | OK |
| E5b | as E5, server answers `200` with an empty body | OK | OK | OK | OK | OK | OK |
| E5c | as E5, server answers `206` with an empty body | OK | OK | OK | OK | OK | OK |
| E6 | complete `.temp`, server ignores `Range` | 1st fails, `.temp` deleted, 2nd OK | same | same | same | same | same |
| E7 | `.temp` holds garbage, server honours `Range` | 1st fails, `.temp` deleted, 2nd OK | same | same | same | same | same |
| E8 | `git clone` cut at 50%, then clone again | clone dir deleted, **restarts from 0** | same | same | same | same | same |
| E9 | URI pack already installed from an earlier fetch | re-downloads it | re-downloads | re-downloads | re-downloads | re-downloads | **skips download** |
| E10 | stale `pack-<hash>.keep` from a crashed fetch | 1st fails, 2nd OK | same | same | same | 1st OK | 1st OK |

---

## Findings and what Aspen must do

Each finding names the scenario rows that prove it.

### Pack format

1. **Every URI pack must be self-contained** (C1, C2, C3, C6). A pack whose
   deltas refer to objects outside itself fails on every client. 2.30 cannot
   resolve the deltas. 2.31 and later complete the pack locally, which
   changes its hash, so the hash check fails. Advertising the
   post-completion hash (C5) only works while the client's zlib settings
   match the server's guess (C6 breaks it), so it is not a strategy.
   **Consequence:** `git push` sends thin packs by default (deltas against
   the advertised trunk). Aspen must *complete* a thin pushed pack (append
   the missing base objects, rewrite the object count, recompute the
   trailer, as `index-pack --fix-thin` does) before that pack can be named
   in a `packfile-uris` response. The original "packs are stored unchanged"
   rule therefore becomes "packs are stored self-contained". See the plan
   change below, and "Completing a thin push" for how ingest does it.
2. **The advertised hash is the pack's trailing SHA-1** (B1; D1 shows a
   mismatch is rejected). The client also uses it to name the partial file
   (`objects/pack/pack-<hash>.pack.temp`). So `packs.checksum` *is* the
   packfile-URI identity, and the bytes behind it must never change.
3. **REF_DELTA inside one pack is fine** for stock clients (B9). Only
   OpenAI's parallel indexer needs REF_DELTA-free packs, and it enforces
   that (C4). Never advertise `no-ref-delta` unless *every* pack in the plan
   is REF_DELTA-free. Record per pack whether it contains REF_DELTA, so
   this can be turned on later: it made 51 packs index about 4x faster (B8).
4. **Overlapping packs are fine** (B4). The planner may over-select, and
   duplicate objects across URI packs cost bytes, not correctness.
5. **Pack count is a latency cost, not a correctness limit** (B7). 51 URIs
   worked on every client. Each URI is one `http-fetch` plus one
   `index-pack` process, run serially (about 10–45 ms each locally). This is
   the case for keeping compaction levels deep. Start with a soft cap of 64
   URIs per response and fall back to a stitched pack above it; tune the cap
   in C1.

### Response shape

6. **Always send a `packfile` section**, even when it is empty (B6). A
   response with URIs and no `packfile` section fails on every client.
7. **Send only URIs whose scheme the client listed** (A4). Clients do not
   filter: a client that asked for `https` only still downloads an `http`
   URI. Filtering is the server's job, because it is what keeps tokens off
   cleartext.
8. **Clients ask for URIs only when the user opts in.** They need
   `fetch.uriprotocols` and protocol v2 (A1–A3), and the non-C clients never
   ask. So the stitched pack (S1) is the default path for almost everyone,
   and URIs are the fast path for configured fleets (CI, agents), as the plan
   already assumes.
9. **Prefer an empty inline pack.** Since 2.31, a fetch with packfile URIs
   leaves a `.keep` on its inline pack (B3, B12), and `git gc` never repacks
   kept packs. With an empty inline pack that is one shared 32-byte empty
   pack; with a real inline pack it is a new kept pack on every fetch.
   (Client bug, still present in 2.56 and the fork. Worth reporting
   upstream; that is out of scope here.)

### URLs and the blob server

10. **URLs must not redirect** (B10). By default (`http.followRedirects=initial`)
    clients do not follow a redirect on a pack URL and fail with "early EOF".
    Presign the final object URL, and configure any CDN to serve it directly.
11. **URLs must carry their own authorization** (D8, D9). Upstream clients
    send no credentials to a pack URL, not even to the repo's own host.
    Use presigned or signed-token URLs. Since 2.35, git redacts the URL path
    in errors, but 2.30 prints the full URL in error messages (E1, D3), so
    keep URL lifetimes short. Short lifetimes are safe for resume (next
    finding).
12. **Resume is keyed by pack hash, not URL** (E1, E2, E3, E11). After a cut
    connection, Ctrl-C or SIGKILL, the next `git fetch` sends
    `Range: bytes=<partial size>-` even when the URL is a fresh presign. Mint
    new URLs on every fetch.
13. **The blob server must honour `Range`** (E4, E6). If it ignores `Range`,
    the client appends the full body to its partial file, fails, deletes it,
    and only the next fetch succeeds. S3, GCS, Azure Blob, R2, MinIO and
    CloudFront all honour `Range`. `aspen-express` and the filesystem blob
    backend must too.
14. **Do not answer a resume at end-of-file with `416`** (E5, E5b, E5c). When
    the download finished but indexing died (Ctrl-C during `index-pack`, or
    an OOM kill), the `.temp` file is complete. The next fetch asks for
    `bytes=<size>-`. S3 and GCS answer `416`, and every client up to 2.55
    fails on that **every time**, until someone deletes the `.temp` file.
    2.56 accepts `416` (upstream commit `85f4f04f82`). Answering `200` or
    `206` with an empty body works on every version. **Consequence:**
    `aspen-express` must answer a range that starts at or past the end with
    `206` and an empty body. Direct-to-bucket presigned URLs cannot do this,
    and Debian 12 ships git 2.39, so a fleet on distro git would wedge with
    no automatic recovery. Aspen therefore gives direct-to-bucket URIs only
    to clients whose `agent=` capability says `git/2.56` or later (see
    "Who gets direct-to-bucket URIs" below).

### Failure behaviour

15. **Failures never move refs** (all D and E rows). Bad hash, missing
    objects, HTTP errors and interrupts all leave `origin/main` where it was,
    and `fsck --strict` passes on every success. Packfile URIs cannot make a
    client repository inconsistent; at worst a fetch fails.
16. **Stock clients do not retry a URI download** (D3–D8). One `503` or
    `429` fails the whole fetch, though the next fetch resumes. Only the
    OpenAI fork retries (`5xx`, `429`, `Retry-After`, and `401` with a
    credential helper). So blob serving availability is fetch availability:
    another reason to put `aspen-express` or a CDN in front of the bucket.
17. **`git clone` is not resumable** (E8). An interrupted clone deletes the
    directory and starts from zero. For large repositories, document
    `git init` + `git remote add` + `git fetch`, which resumes like any
    fetch. `aspen clone` could wrap this later.
18. **Re-downloads of installed packs** (E9). Stock clients re-download a URI
    pack they already have; the fork skips it. The planner should avoid
    naming packs at or below the client's haves. It already does by design.

---

## Completing a thin push

This is how ingest produces the final stored bytes. It is synchronous, so
every `packs` row is self-contained for every reader (URI planner, stitcher,
compactor) with no flag to check.

1. **Spool and validate (pass 1).** The incoming pack streams to local disk
   while `gix-pack` validates it, checks SHA-1 collisions, verifies the
   client's trailer and builds the index. Delta bases that are not in the
   pack are collected as the *missing-base set*.
2. **Look up and append missing bases.** Each missing base oid is found with
   the base-object lookup below. Its content is read from that pack
   (resolving its delta chain there; stored packs are self-contained, so
   the chain is local to that pack) and appended to the spool file as a
   full, undeltified object, as `index-pack --fix-thin` does. If a base
   cannot be found, the push fails with `unpack error` and no ref moves.
3. **Hash and upload (pass 2).** The new object count is known, so the
   header (bytes 8–12) is written first. One sequential read of the spool
   file feeds both the SHA-1 and a multipart upload; the trailer (the new
   SHA-1, which becomes `packs.checksum`) is the last part. The `.idx` is
   uploaded next to it (`packs/<checksum>.idx`).
4. **Commit.** Only after the pack and its index are durable does the push
   transaction insert the `packs` and `ref_versions` rows and move the ref.
   A crash before that leaves an orphan blob for GC, never a ref pointing at
   an incomplete pack.

A non-thin push takes the same path with an empty missing-base set (the
header and trailer are unchanged). One code path is simpler to test than an
"upload during receive when not thin" fast path; that optimisation can come
later if B1's numbers ask for it.

**Cost.** Local disk equal to the push size, and push latency of
`receive + lookup + recompress missing bases + upload`, since the upload no
longer overlaps the receive. The alternative, asynchronous completion with
a `packs.self_contained` flag, would hide that latency but make every
planner and the stitcher reason about incomplete packs, and a ref could
point at a pack nobody may serve yet. S4 (ingest numbers) and B1
(receive-pack) must measure the completion cost: spool throughput, missing
bases per push for typical agent pushes, lookup time cold and warm, and
end-to-end push latency against `git receive-pack`.

## Base-object lookup

Completing a thin pack needs, for each missing base oid, the pack and
offset where that object lives. `commit_index` covers commits only, so it
cannot answer this. S2's object-existence check (for example a ref update
to an oid that is not in the pushed pack) needs the same lookup and should
reuse this one.

* **Where to look.** A thin pack's bases are objects reachable from what
  was advertised, which is trunk only. The search set is the packs a fresh
  clone of the trunk ref version would be served: the planner's covering
  set for `wants = trunk tip, haves = none` (fetch path step 2). Look in
  the version advertised on this connection first, then in the push's
  dependency (the trunk version current at commit, per S2) if it differs.
  Because every stored pack is self-contained, a base found in any of
  those packs is usable regardless of which version it came from.
* **How.** Probe each pack's `.idx` (fan-out table, then binary search),
  newest level first: uncompacted L0 packs, then L1 to L4 covers. Stop at
  the first hit. `.idx` files are immutable and keyed by checksum, so they
  are cached on local disk and memory-mapped; a cold cache downloads them
  from the blob store.
* **Cost per push.** `M` missing bases (the bases of the changed blobs and
  trees, usually tens) × `P` packs probed × `O(log N)` comparisons per
  probe. `P` is bounded by compaction: uncompacted L0 packs since the last
  L1 cover plus a few covers per level, the same `O(log Δ)` bound as fetch
  planning. With warm caches that is microseconds. Cold, it is up to `P`
  `.idx` downloads (about 28 bytes per object each); compaction keeps `P`
  small, so this is a cache-sizing question for B1, not an algorithmic one.
* **Rejected for v1: a global `oid → pack` table.** One row per object in
  Postgres is billions of rows for a large monorepo and a write per object
  on every push. A multi-pack index per cover is a possible later
  optimisation if `P` grows.

## Who gets direct-to-bucket URIs

Finding 14 makes direct presigned bucket URLs unsafe for git 2.55 and
earlier. Rule for B2:

* A client gets direct-to-bucket URIs only when its `agent=` capability
  parses as `git/<major>.<minor>` with a version of 2.56 or later (Apple
  and Windows builds append suffixes after the version; parse the prefix).
* Every other opted-in client gets URIs on an endpoint that answers a range
  at or past the end with an empty `206`: `aspen-express`, or a CDN
  configured to do so. When no such endpoint is configured, it gets the
  stitched pack.
* An unparseable agent counts as old.
* The OpenAI fork cannot be singled out. It reports `git/2.55.0.921.g…`
  (see the version table in [results/matrix.md](results/matrix.md)), the
  same shape as any 2.55 development build, so it is treated as 2.55. It
  handles `416` (E5), so this only costs it the direct path.
* The agent string is self-reported. This is a reliability choice, not a
  security control: a client that lies about its version can only hurt its
  own fetches.

## What N1 should take from this

* `packs.checksum`: the 20-byte SHA-1 trailer of the exact stored bytes.
  Unique. It is the packfile-URI hash and never changes for a given blob.
  Derive the blob key from it (`packs/<checksum>.pack`), so a URL can never
  serve different bytes under the same hash.
* `packs`: every row is self-contained, an invariant enforced at ingest by
  completing thin pushes synchronously ("Completing a thin push"). A row
  exists only once its pack and `.idx` are durable. Add
  `has_ref_delta bool` (or a count) so that `no-ref-delta` can be advertised
  safely later.
* An object-location contract in `aspen-core`, used by thin-pack completion
  and S2's existence check: given a ref version and an oid, return
  `(pack_id, offset)` or "not found", by probing the `.idx` of the ref
  version's covering packs ("Base-object lookup"). Blob layout:
  `packs/<checksum>.pack` and `packs/<checksum>.idx`.
* `BlobStore` contract: `presign(key, ttl) -> Url` returns a direct URL
  (no redirect) that honours `Range`. Any Aspen-served blob endpoint answers
  a range starting at or past the end with `206` and an empty body. The
  blob configuration says whether presigned URLs come from a bucket
  (direct, may answer `416`) or from such an endpoint, because B2 hands
  direct-bucket URIs only to git 2.56 and later.
* Fetch response contract (B2): URIs only for schemes the client listed;
  always a `packfile` section, empty when everything is in URIs; a soft cap
  on URI count, with a stitched pack beyond it.
* No change to the "stitched pack by default" decision. Only opted-in
  clients ever use this path.

## Plan changes made in this PR

* Section 2, push path step 2: a pushed pack is stored **self-contained**
  (a thin pack is completed synchronously during ingest: spool, append
  missing bases, hash and upload, then commit), not "unchanged".
* Section 2, fetch path step 3: the packfile-URI rules above, including who
  gets direct-to-bucket URIs.
* Section 3, S4 and B1: measure the cost of completion.
* Section 8, decisions log: rows for the above.

## Limits of this spike

* All URIs were `http://` on localhost. HTTPS goes through the same curl
  code path but was not exercised; nor were CDN or real S3 responses (the
  S3/GCS `416` behaviour is from their documentation).
* Packs were tens of kilobytes. Resume is byte-offset based and size
  independent in the code, but multi-GB downloads were not tried (S4 covers
  ingest at that size).
* Apple Git was not tested (no macOS machine in this spike). It tracks
  upstream releases.
* JGit, libgit2, go-git and gitoxide were checked by reading source, not by
  running them.
* The clients ran on Linux only. Windows has its own `.temp` file-sharing
  code (visible in 2.56's `http.c`) that was not exercised.
