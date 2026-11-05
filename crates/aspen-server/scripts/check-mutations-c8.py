#!/usr/bin/env python3
"""Breaks each guard node C8's tests rely on (exact cached shallow packs:
which requests use them, how a pack is keyed, built and shared, and what
garbage collection does with it), one at a time, and shows that a test
fails without it. Run from the repository root:

    python3 crates/aspen-server/scripts/check-mutations-c8.py [X1 X2 ...]

Each mutation is an exact text replacement in one file; the file is
restored afterwards. A mutation whose pattern no longer matches is
reported, so the list fails loudly when the code it targets changes.
Exit status 1 if any mutation survives or no longer applies.

The store side (migration 0006, `set_shallow_pack`, eviction, the sweep)
and the pack side (`write_repack_subset`, `shallow_objects`) were checked
the same way when they were written; their mutations are listed in the C8
rows of docs/PLAN.md's decisions log."""
import subprocess, sys

EX = 'crates/aspen-server/src/upload_pack/exact.rs'
SH = 'crates/aspen-compact/src/shallow.rs'
FE = 'crates/aspen-server/src/upload_pack/fetch.rs'
GC = 'crates/aspen-compact/src/gc.rs'

T = ["-p", "aspen-server", "--test", "shallow_packs"]
F = ["-p", "aspen-server", "--test", "fetch"]

muts = [
 ("X1 a want that is not its version's tip is built and recorded for that version", EX,
  "if version.value.tip() != Some(want.id) {",
  "if false {", T + ["other_shallow_requests_get_the_full_closure"]),
 ("X2 deepen-relative served from exact packs", EX,
  "if request.deepen_relative || wants == 0",
  "if wants == 0", T + ["other_shallow_requests_get_the_full_closure"]),
 ("X3 the region is one commit deeper than the boundary sent", EX,
  "shallow::by_depth(&mut objects, vec![head], depth.get()).await?",
  "shallow::by_depth(&mut objects, vec![head], depth.get() + 1).await?",
  T + ["a_depth_one_fetch_storm_is_served_exact_packs_within_stock_bytes"]),
 ("X4 every request builds its own pack (no single flight)", EX,
  "Some(Build::Running(rx)) => rx.clone(),",
  "Some(Build::Running(_)) => return Ok(Err(ShallowFallback::NoBuildSlot)),",
  T + ["a_depth_one_fetch_storm_is_served_exact_packs_within_stock_bytes"]),
 ("X5 a failed build is retried by the next request", EX,
  "Some(Build::Settled(at)) if at.elapsed() < cache.config.retry_after => {",
  "Some(Build::Settled(at)) if at.elapsed() < Duration::ZERO => {",
  T + ["a_build_that_records_nothing_falls_back_and_is_not_retried_at_once"]),
 ("X6 a want whose closure is already exact fails the whole request", EX,
  "Some(Delivered::Closure) => Ok(Ok(Share::Closure)),",
  "Some(Delivered::Closure) => Ok(Err(ShallowFallback::BuildRecordedNothing)),",
  T + ["several_wants_tags_and_v0_are_served_exact_packs"]),
 ("X7 depths over max_depth served from exact packs", EX,
  "(n.get() <= self.config.max_depth)",
  "(n.get() <= u32::MAX)", T + ["other_shallow_requests_get_the_full_closure"]),
 ("X8 a byte-identical pack at another level registered as a new row", SH,
  "Some(existing) if existing.pack.level() != level.into() => existing,",
  "Some(existing) if false => existing,",
  T + ["the_compactor_reuses_a_byte_identical_pack_and_skips_needless_builds"]),
 ("X9 a build whose region reaches the whole closure stores a pack anyway", SH,
  "    if keep.len() == union.len() {",
  "    if false {",
  T + ["the_compactor_reuses_a_byte_identical_pack_and_skips_needless_builds"]),
 ("X10 shallow fetches never use exact packs", FE,
  "let rows = match exact::packs(server, repo, request, wants).await? {",
  "let rows = match None::<Vec<PackRecord>> {",
  F + ["shallow_fetches_match_stock_git"]),
 ("X11 garbage collection never evicts shallow pack recordings", GC,
  ".evict_shallow_packs(self.config.shallow_ttl, self.config.gc_batch)",
  ".evict_shallow_packs(self.config.shallow_ttl, 0)",
  T + ["evicted_packs_are_swept_and_built_again"]),
]

only = sys.argv[1:]
bad = False
for name, f, old, new, args in muts:
    if only and not any(name.startswith(o + " ") for o in only):
        continue
    src = open(f).read()
    if src.count(old) != 1:
        print((name, "PATTERN NOT FOUND x%d" % src.count(old)), flush=True)
        bad = True
        continue
    open(f, 'w').write(src.replace(old, new))
    try:
        r = subprocess.run(["cargo", "test", "--locked"] + args[:-1] + ["--", args[-1]],
                           capture_output=True, text=True, timeout=1500)
        out = r.stdout + r.stderr
        if "error[" in out or "could not compile" in out:
            verdict = "COMPILE ERROR"
            bad = True
        elif r.returncode != 0 and "test result: FAILED" in out:
            verdict = "KILLED (test fails)"
        elif r.returncode == 0:
            verdict = "SURVIVED"
            bad = True
        else:
            verdict = "ERROR exit %d" % r.returncode
            bad = True
    finally:
        open(f, 'w').write(src)
    print((name, verdict), flush=True)
sys.exit(1 if bad else 0)
