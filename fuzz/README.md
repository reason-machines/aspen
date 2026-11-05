# Fuzzing

Aspen reads bytes from strangers: request paths, SSH commands, compressed
request bodies and identity tokens at the front door, pkt-lines and
requests on every Git connection, packs on every push, and its own stored
packs and tables back from object storage. This directory fuzzes those
surfaces (`docs/PLAN.md`, section 4, item 5, node D3, and issue #139).

## Layout

| Path | What |
|---|---|
| `src/` | The `aspen-fuzz` crate: every target as a plain function `fn(&[u8])` that panics when an invariant breaks, plus the pack generator and the seed generator |
| `libfuzzer/` | The cargo-fuzz harness: one three-line `fuzz_target!` wrapper per target. Its own Cargo workspace, because it needs a nightly toolchain and `libfuzzer-sys` |
| `corpus/<target>/` | Seed inputs, written by `aspen-fuzz seeds` |
| `regressions/<target>/` | Inputs that once failed. Kept forever |
| `tests/corpus.rs` | Replays every seed and every regression on stable, checks that `corpus/` is exactly what the seed generator writes, and checks that generated inputs reach every oracle path |
| `run.sh` | Runs targets under libFuzzer |

`aspen-fuzz` is a tooling crate in the main workspace, so `cargo fmt`,
`clippy` and `cargo test` cover it in required CI. Only the wrappers in
`libfuzzer/` need nightly.

## Targets

| Target | Surface | What must hold |
|---|---|---|
| `pkt_line` | `aspen_proto::pkt` | `decode` over a buffer and `PktReader` over the same bytes in random chunks see the same packets and end the same way; the reader's remainder after any packet is exactly the bytes after it; payloads re-encode to themselves; side-band demux, capability lists, `agent=`, `GIT_PROTOCOL` and service names parse without panicking; `one_line` never leaves a control character or more than 4,096 bytes |
| `receive_request` | v0/v1 push request (`receive::RequestParser`), push options, `ReportStatus` | A parsed request's report, with random statuses and with reasons and notes full of newlines and control characters, writes, frames, and has exactly one `ok`/`ng` line per command, invalid names at their positions, so nothing a client or hook writes can forge a report line |
| `v2_command` | v2 `ls-refs` and `fetch` (`v2::CommandParser`, `FetchResponse`) | A parsed `fetch` answered in the server's order (acknowledgments, `shallow-info`, `packfile-uris`, pack, progress, error) is never refused, and the output frames |
| `upload_v0` | v0/v1 upload-pack (`upload_v0`), stateless and stateful | The whole chain (header, shallow section, negotiation rounds with random shares and `ready`, pack) answers without being refused; real libgit2, go-git and git transcripts seed it |
| `pack_bytes` | Raw bytes as a pushed pack and as a stored pack | Whatever `receive` plus `complete` accepts passes every stored-pack check (below); a pack `verify_pack` accepts within the push limits (object count and pack size) must be received and completed, to the same objects |
| `pack_ingest` | Push ingest on generated packs | A model predicts the outcome of packs with forward bases, REF_DELTA cycles, thin bases the lookup may lack, resent objects, repeated objects, zip bombs over the object limit and long chains. Valid packs must be accepted and store exactly their objects plus the thin bases they needed; repeated objects, unresolvable bases, objects over the limit and deltas with less than the 4 bytes of data Git requires (its `DELTA_SIZE_MIN`) must be refused. Some packs also carry lies, each of which makes the pack certainly invalid: an entry's header size changed to a different value, its zlib stream cut short, a flipped byte of its zlib checksum, or a header count other than the entry count. A pack with a lie must be refused |
| `pack_formats` | Stored `.idx`, entry tables, entry headers, commits | Parsers of what comes back from object storage never panic, and decoded tables and headers re-encode to themselves |
| `stitch` | The stitcher | Segments that repeat objects stitch into a pack that passes `verify_pack` and holds exactly their union; the streaming `Stitcher` fed in random chunks writes the same bytes as `stitch_all`; a flipped byte, a short or long segment, or an entry table whose IDs disagree with its `.idx` fails the stitch |
| `planner` | `CoverPlanner` | Over random histories (forced and scoped pushes, deletions, shared and empty packs, chained and unchained covers, versions hidden from the loaded view), no plan loses an object the client lacks, and without a chain skip a plan sends exactly the needed versions' objects (A5's checks, on an abstract object model) |
| `git_route` | `aspen_server::access::GitRoute` | The route of any path and query equals a model written over path segments; a routed path is exactly where its route is served (with or without `.git`, no percent-decoding); a `POST` ignores its query; the SSH transport's `from_repo_path` routes the repository path the same |
| `ssh_command` | The SSH `exec` command parser (`aspen_server::fuzzing::ssh_command`), `GIT_PROTOCOL` | A command quoted as Git quotes it gets the model's exact service and path or refusal; any accepted command is one a POSIX shell would split into the same words with nothing expanded (a model of the shell, not of Aspen), its path has no leading `/` or `~` and no control character, and it routes as over HTTP; `GIT_PROTOCOL` picks the highest valid `version=` |
| `gzip_body` | Upload-pack's bounded body reader and receive-pack's gzip push reader (`aspen_server::fuzzing`) | On gzip streams it builds (cut short, bit-flipped, with trailing bytes or a second member, zip bombs), each reader gives the model's answer against its limit, the push meter's cap and the sent-bytes ceiling, a damaged stream never inflates to anything but its body, and the meter is charged at least the larger of the bytes sent and inflated; on any input the two readers inflate exactly the same bodies |
| `jwt` | `aspen_auth::jwt` | Tokens the target signs (ES256 and ES384 keys generated per process; headers and payloads written field by field, with wrong types, repeated fields, `crit`, refused algorithms, `kid` mismatches, times around the leeway, damaged signatures) get the model's exact error, in the verifier's order of checks, or its exact claims; `unverified_issuer` and `looks_like_jwt` match their models; a raw token, which no key signed, is refused before any claim is read |

`pack_ingest` gives a REF_DELTA cycle, and every delta that leads into
one, a single object kind. With mixed kinds, a delta resolved against the
lookup's copy of a cycle member takes that copy's kind and becomes a
different object than the model expects, and the model does not follow
that; mixed-kind cycles are reached only through `pack_bytes`, which has no
model.

`pack_ingest`, `stitch`, `planner` and the four front-end targets report
which oracle paths each input took (`aspen_fuzz::Reached`). Required CI runs a fixed sequence of a few
hundred generated inputs through them and fails unless every path is
reached: exact acceptance, each kind of refusal, and the damaged stitch;
for the front ends, every route refusal, both SSH refusals, every body
refusal and each JWT error, every claim check included. A
generator change that stops producing, say, refused lies fails CI rather
than leaving that check unexercised.

The stored-pack checks, applied to everything ingest accepts: `verify_pack`
accepts it (self-contained, base-first, no object twice), the `.idx` and
entry table ingest wrote equal the ones rebuilt from the bytes, the `packs`
row validates, and commit extraction and the reachability graph run over it.

The front-end targets read their first byte as a mode: 3 (or any byte
equal to it modulo 4) passes the rest through as raw bytes, which only the
properties that hold for any input check; anything else builds a request
from decisions, whose exact answer the target's model predicts. `aspen-server`
keeps the SSH command parser and the body readers private; its `fuzzing`
feature (`aspen_server::fuzzing`, never enabled by the binary) exposes
them to this crate unchanged.

Limits in every target are small (1 MiB per object, 16 MiB inflated per
pack), and `run.sh` gives libFuzzer 2 GiB of RSS and 1 GiB per allocation,
so a size check that is missing shows up as an out-of-memory crash.

## Run it

Replay the corpus on stable, as required CI does (under a second):

```bash
cargo test -p aspen-fuzz
```

Fuzz. Install a nightly toolchain and cargo-fuzz once:

```bash
rustup toolchain install nightly-2026-11-02 --profile minimal
cargo install cargo-fuzz --version 0.13.2 --locked
```

Then run one target, or all of them one after another:

```bash
fuzz/run.sh -t 300 pkt_line          # 5 minutes on one target
fuzz/run.sh -t 600 -j 8              # every target, 10 minutes each, 8 processes
```

`run.sh` keeps a working corpus in `$TMPDIR/aspen-fuzz/corpus/<target>`
(seeded from `corpus/` and `regressions/`) and writes failing inputs to
`$TMPDIR/aspen-fuzz/artifacts/<target>`. It exits non-zero if any target
found one. To call cargo-fuzz directly:

```bash
cargo +nightly-2026-11-02 fuzz run --fuzz-dir fuzz/libfuzzer -O planner
```

## When a target fails

1. Reproduce on stable, which prints the broken invariant:
   `cargo run -p aspen-fuzz -- run <target> <artifact>`.
   For `pack_ingest`, `ASPEN_FUZZ_EXPLAIN=1` also prints the model's view
   (each entry's form, ID and whether it resolves), and with it
   `ASPEN_FUZZ_DUMP=<dir>` writes the generated pack to `<dir>/pack`, for
   `git index-pack` or `git verify-pack -v`.
2. Decide whether the code or the target is wrong. A target's model can be
   wrong too; fix whichever is.
3. Copy the input to `regressions/<target>/` with a name that says what
   it was, so required CI replays it from then on.

An input to a generated target (`pack_ingest`, `stitch`, `planner`, and
the front-end targets outside raw mode) is a list of decisions for its
generator, not a pack, a history or a request. It builds
the same case only while the generator reads its bytes the same way, so
any change to a generator can turn a saved regression into a different
case. Replaying it still passes or fails, but check with
`ASPEN_FUZZ_EXPLAIN=1` (for `pack_ingest`) what it builds now, and rename
or replace it when it no longer builds the case its name describes.
`regressions/pack_ingest/ref-delta-cycle-the-lookup-also-holds` is one: it
was saved as a mixed-kind REF_DELTA cycle, and since the generator gave
cycles one kind it builds a two-entry blob cycle whose members the lookup
also holds.

## Adding a target

1. Write `pub fn <name>(data: &[u8])` in `src/` and list it in `TARGETS`
   (`src/lib.rs`).
2. Add `libfuzzer/fuzz_targets/<name>.rs` (copy another wrapper) and its
   `[[bin]]` in `libfuzzer/Cargo.toml`.
3. Add seeds in `src/seeds.rs` and regenerate:
   `cargo run -p aspen-fuzz -- seeds fuzz/corpus`.

`tests/corpus.rs` fails until all three are done.

## CI

- **Required** (`ci.yml`, `test` job): `cargo test --workspace` replays the
  corpus and every regression.
- **Nightly** (`.github/workflows/fuzz.yml`): one GitHub-hosted job runs
  every target for 180 seconds under AddressSanitizer, on a working corpus
  kept in the Actions cache, and uploads failing inputs as
  `fuzz-artifacts`: 13 targets, 39 minutes of fuzzing and about 45
  runner-minutes with the build. It is kept short because the
  organization's hosted-runner minutes are limited. A manual run takes up to 600 seconds
  per target and 4,800 seconds in all (the job times out at 95 minutes);
  longer campaigns run with `run.sh` on a machine of your own.
- **Launch gate** ("fuzzers ran 24 h clean", `docs/PLAN.md` section 7):
  24 hours of wall-clock fuzzing across all targets, every target fuzzed
  for at least 2.5 hours, and no failing input. Run
  `fuzz/run.sh -t 9600 -j <cores>` on a machine of your choice: it fuzzes
  the 13 targets one after another for 9,600 seconds each (2 h 40 min,
  about 35 hours in all) and exits non-zero if any target found a failing
  input. Keep its output as the evidence. With more targets, keep `-t` at
  9,000 or more and the total at 24 hours or more.

## Known open findings

None. #151 (thin-pack
completion with a net-zero shift) is fixed; its six inputs are
`regressions/pack_ingest/net-zero-thin-completion-*`.
#162 (ingest stored a
delta with less than 4 bytes of data, which `git index-pack` refuses) is
fixed; its input is `regressions/pack_ingest/short-delta-empty-result`.

## Not covered here

- **SHA-1 collisions.** Pack trailers and object IDs are hashed with
  collision detection (`sha1dc` in `gix-hash`), but a fuzzer cannot
  produce a collision, so that path is not exercised here;
  #129 adds a unit
  test for it.
- **Git's maximum delta depth (4,095).** A chain that long costs thousands
  of entries per run; `aspen-pack`'s own tests pin the limit, and
  `pack_ingest` builds chains of up to 320 deltas.
