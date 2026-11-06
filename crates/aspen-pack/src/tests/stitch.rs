//! The stitcher: S1's invariants on every output (each ID at most once,
//! every base earlier, never thin, header count exact), single-pass
//! behaviour, the forward-base and short-read failures, and the delta chain
//! depth keep-first can build across segments.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use aspen_core::error::{FetchError, StoreError};
use aspen_core::id::ObjectId;
use aspen_core::pack::EntryTable;

use crate::stitch::{SegmentIds, StitchPlan, Stitcher, stitch_all};
use crate::testutil::{Base, Obj, Spec, build_pack, git_index_pack, pack_id, scratch, table_of};
use crate::verify::verify_pack;

/// SplitMix64, as testkit uses: a printed seed replays exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// A segment: bytes, entry table, the IDs it holds.
#[derive(Clone)]
struct Seg {
    bytes: Vec<u8>,
    table: EntryTable,
}

impl Seg {
    fn new(bytes: Vec<u8>, dir: &Path) -> Self {
        let table = table_of(&bytes, dir);
        Self { bytes, table }
    }
    fn ids(&self) -> BTreeSet<ObjectId> {
        self.table.entries().iter().map(|e| e.id).collect()
    }
    /// `(id, offset)` ascending by ID, as the `.idx` lists them.
    fn pairs(&self) -> Vec<(ObjectId, u64)> {
        let mut v: Vec<_> = self
            .table
            .entries()
            .iter()
            .map(|e| (e.id, e.offset))
            .collect();
        v.sort_unstable();
        v
    }
}

fn stitch(segs: &[&Seg]) -> Result<(Vec<u8>, crate::stitch::StitchOutcome), FetchError> {
    stitch_all(
        segs.iter()
            .enumerate()
            .map(|(i, s)| (pack_id(i as i64 + 1), s.bytes.clone(), s.table.clone()))
            .collect(),
    )
}

/// Every invariant of a stitched pack, checked independently: it indexes
/// with no lookup (never thin), no ID repeats and every base is earlier
/// (`verify_pack`), the header count matches, and it holds exactly the
/// union of the segments' objects.
fn check_output(out: &[u8], segs: &[&Seg], dir: &Path) -> u32 {
    let p = dir.join(format!("stitched-{}.pack", crate::sha1(out)));
    std::fs::write(&p, out).unwrap();
    let v = verify_pack(
        &p,
        dir,
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap_or_else(|e| panic!("stitched pack: {e}"));
    let got: BTreeSet<ObjectId> = v.table.entries().iter().map(|e| e.id).collect();
    let want: BTreeSet<ObjectId> = segs.iter().flat_map(|s| s.ids()).collect();
    assert_eq!(got, want);
    assert_eq!(
        u32::from_be_bytes(out[8..12].try_into().unwrap()) as usize,
        want.len()
    );
    std::fs::remove_file(p).unwrap();
    v.max_delta_depth
}

/// Objects in families, so deltas within a family are meaningful.
fn universe(rng: &mut Rng, n: usize) -> Vec<Obj> {
    (0..n)
        .map(|i| {
            let family = i % 4;
            let mut s = format!("family {family}\n").repeat(30);
            for _ in 0..(1 + rng.below(6)) {
                s.push_str(&format!("{} {}\n", i, rng.next()));
            }
            Obj::blob(s)
        })
        .collect()
}

/// A random stored-pack layout over a subset of `universe`: self-contained,
/// base-first and without repeats, mixing full entries, OFS_DELTAs and
/// in-pack REF_DELTAs at random.
fn random_pack(rng: &mut Rng, universe: &[Obj]) -> Vec<u8> {
    let mut picked: Vec<usize> = (0..universe.len()).filter(|_| rng.below(3) != 0).collect();
    if picked.is_empty() {
        picked.push(rng.below(universe.len()));
    }
    // Shuffle.
    for i in (1..picked.len()).rev() {
        picked.swap(i, rng.below(i + 1));
    }
    let mut specs = Vec::new();
    for (pos, &u) in picked.iter().enumerate() {
        let o = universe[u].clone();
        specs.push(match (pos, rng.below(3)) {
            (0, _) | (_, 0) => Spec::Full(o),
            (_, 1) => Spec::Ofs(rng.below(pos), o),
            _ => Spec::Ref(Base::Entry(rng.below(pos)), o),
        });
    }
    build_pack(&specs)
}

#[test]
fn random_plans_stitch_to_valid_packs() {
    // 300 random plans of 1 to 5 segments (repeats and any order) from a
    // pool of random base-first packs over overlapping object sets.
    let dir = scratch();
    let seed = 0xa2_5717c4;
    let mut rng = Rng(seed);
    let uni = universe(&mut rng, 24);
    let pool: Vec<Seg> = (0..8)
        .map(|_| Seg::new(random_pack(&mut rng, &uni), dir.path()))
        .collect();
    let mut with_dups = 0;
    let mut deepest = 0;
    for round in 0..300 {
        let n = 1 + rng.below(5);
        let segs: Vec<&Seg> = (0..n).map(|_| &pool[rng.below(pool.len())]).collect();
        let (out, outcome) =
            stitch(&segs).unwrap_or_else(|e| panic!("seed {seed} round {round}: {e}"));
        let depth = check_output(&out, &segs, dir.path());
        assert_eq!(outcome.max_delta_depth, depth, "round {round}");
        assert_eq!(outcome.size, out.len() as u64);
        assert_eq!(&out[out.len() - 20..], outcome.checksum.as_bytes());
        if outcome.dropped > 0 {
            with_dups += 1;
        }
        deepest = deepest.max(depth);
        if round < 20 {
            git_index_pack(&out, dir.path(), true)
                .unwrap_or_else(|e| panic!("round {round}: git index-pack --strict: {e}"));
            if let Some(old) = old_git() {
                git_index_with(&old, &out, dir.path())
                    .unwrap_or_else(|e| panic!("round {round}: git 2.30: {e}"));
            }
        }
    }
    assert!(
        with_dups > 200,
        "the pool overlaps: {with_dups} plans had duplicates"
    );
    assert!(deepest > 1);
}

#[test]
fn output_does_not_depend_on_how_bytes_are_fed() {
    let dir = scratch();
    let mut rng = Rng(7);
    let uni = universe(&mut rng, 12);
    let a = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let b = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let (whole, _) = stitch(&[&a, &b, &a]).unwrap();
    for chunk in [1, 2, 7, 4096] {
        let plan = plan_of(&[&a, &b, &a]);
        let mut out = Vec::new();
        let mut s = Stitcher::new(plan, &mut out);
        for seg in [&a, &b, &a] {
            s.begin_segment(seg.table.clone()).unwrap();
            for c in seg.bytes.chunks(chunk) {
                s.feed(c, &mut out).unwrap();
            }
            s.end_segment().unwrap();
        }
        s.finish(&mut out).unwrap();
        assert_eq!(out, whole, "chunk size {chunk}");
    }
}

fn plan_of(segs: &[&Seg]) -> StitchPlan {
    StitchPlan::new(
        segs.iter()
            .enumerate()
            .map(|(i, s)| SegmentIds {
                pack: pack_id(i as i64 + 1),
                checksum: *s.table.checksum(),
                ids: s.pairs().into_iter(),
            })
            .collect(),
    )
    .unwrap()
}

#[test]
fn a_single_segment_passes_through_unchanged() {
    let dir = scratch();
    let mut rng = Rng(11);
    let uni = universe(&mut rng, 10);
    let a = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let (out, outcome) = stitch(&[&a]).unwrap();
    assert_eq!(out, a.bytes);
    assert_eq!((outcome.dropped, outcome.rewritten_headers), (0, 0));
}

#[test]
fn a_dropped_base_moves_the_kept_delta_onto_the_first_copy() {
    // S1's case: segment 1 holds X full; segment 2 holds X full and Y as an
    // OFS_DELTA on its own X. Y must point at segment 1's X.
    let dir = scratch();
    let x = Obj::blob("x ".repeat(200));
    let y = Obj::blob(format!("{}y", "x ".repeat(200)));
    let filler = Obj::blob("filler ".repeat(100));
    let a = Seg::new(
        build_pack(&[Spec::Full(x.clone()), Spec::Full(filler)]),
        dir.path(),
    );
    let b = Seg::new(
        build_pack(&[Spec::Full(x.clone()), Spec::Ofs(0, y.clone())]),
        dir.path(),
    );
    let (out, outcome) = stitch(&[&a, &b]).unwrap();
    assert_eq!((outcome.dropped, outcome.rewritten_headers), (1, 1));
    check_output(&out, &[&a, &b], dir.path());
}

/// Plans `segs` leaving `out` out and stitches them.
fn stitch_leaving_out(
    segs: &[&Seg],
    out: &[ObjectId],
) -> Result<(Vec<u8>, crate::stitch::StitchOutcome), FetchError> {
    let plan = StitchPlan::leaving_out(
        segs.iter()
            .enumerate()
            .map(|(i, s)| SegmentIds {
                pack: pack_id(i as i64 + 1),
                checksum: *s.table.checksum(),
                ids: s.pairs().into_iter(),
            })
            .collect(),
        out.iter().copied().collect(),
    )
    .map_err(FetchError::Store)?;
    let mut bytes = Vec::new();
    let mut st = Stitcher::new(plan, &mut bytes);
    for s in segs {
        st.begin_segment(s.table.clone())?;
        st.feed(&s.bytes, &mut bytes)?;
        st.end_segment()?;
    }
    let outcome = st.finish(&mut bytes)?;
    Ok((bytes, outcome))
}

/// Invariant (layered shallow answers): every copy of a left-out object is
/// dropped, from every segment, and the header count says so; the output
/// still indexes on its own with stock Git. A kept delta against a
/// left-out entry is refused, never sent thin.
#[test]
fn left_out_objects_are_dropped_from_every_segment() {
    let dir = scratch();
    let x = Obj::blob("x ".repeat(200));
    let y = Obj::blob(format!("{}y", "x ".repeat(200)));
    let z = Obj::blob("z ".repeat(100));
    let w = Obj::blob("w ".repeat(100));
    let a = Seg::new(
        build_pack(&[Spec::Full(z.clone()), Spec::Full(w.clone())]),
        dir.path(),
    );
    let b = Seg::new(
        build_pack(&[
            Spec::Full(x.clone()),
            Spec::Ofs(0, y.clone()),
            Spec::Full(z.clone()),
        ]),
        dir.path(),
    );
    let (out, outcome) = stitch_leaving_out(&[&a, &b], &[z.id()]).unwrap();
    assert_eq!(outcome.left_out, 2, "both copies");
    assert_eq!(outcome.object_count, 3);
    let p = dir.path().join("left-out.pack");
    std::fs::write(&p, &out).unwrap();
    let v = verify_pack(
        &p,
        dir.path(),
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap();
    let got: BTreeSet<ObjectId> = v.table.entries().iter().map(|e| e.id).collect();
    assert_eq!(got, [x.id(), y.id(), w.id()].into_iter().collect());
    git_index_pack(&out, dir.path(), false).unwrap();

    // Leaving out a kept delta's base would make the pack thin: refused.
    let err = stitch_leaving_out(&[&a, &b], &[x.id()]).unwrap_err();
    assert!(is_corrupt(&err), "{err}");
}

#[test]
fn a_forward_base_in_a_stored_pack_fails_the_fetch_retryably_and_pages() {
    let dir = scratch();
    let o: Vec<Obj> = (0..2)
        .map(|i| Obj::blob(format!("{i}").repeat(100)))
        .collect();
    let bad = build_pack(&[
        Spec::Ref(Base::Entry(1), o[1].clone()),
        Spec::Full(o[0].clone()),
    ]);
    // `table_of` would refuse it (it is not base-first), so build the table
    // from gix's .idx without the layout check.
    let p = dir.path().join("bad.pack");
    std::fs::write(&p, &bad).unwrap();
    let table = unchecked_table(&p, dir.path());
    let err = stitch_all(vec![(pack_id(9), bad, table)]).unwrap_err();
    assert_eq!(
        err,
        FetchError::StoredPackNotBaseFirst {
            pack: pack_id(9),
            offset: 12
        }
    );
    let w = aspen_core::error::ToWire::to_wire(&err);
    assert!(w.retryable && w.alert);
}

fn unchecked_table(pack: &Path, dir: &Path) -> EntryTable {
    let out = dir.join("idx-only");
    std::fs::create_dir_all(&out).unwrap();
    let mut r = std::io::BufReader::new(std::fs::File::open(pack).unwrap());
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut r as &mut dyn std::io::BufRead,
        Some(&out),
        &mut gix_utils::progress::Discard,
        &std::sync::atomic::AtomicBool::new(false),
        None::<gix_object::find::Never>,
        gix_hash::Kind::Sha1,
        Default::default(),
    )
    .unwrap();
    let idx = std::fs::read(outcome.index_path.unwrap()).unwrap();
    let idx = crate::idx::IdxV2::parse(&idx, None).unwrap();
    let size = std::fs::metadata(pack).unwrap().len();
    crate::entries::build(std::fs::File::open(pack).unwrap(), size, &idx).unwrap()
}

fn is_corrupt(e: &FetchError) -> bool {
    matches!(e, FetchError::Store(StoreError::Corrupt { .. }))
}

#[test]
fn short_long_and_corrupt_segments_are_corruption_not_truncated_packs() {
    let dir = scratch();
    let mut rng = Rng(3);
    let uni = universe(&mut rng, 8);
    let a = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let run = |bytes: &[u8]| -> Result<(), FetchError> {
        let mut out = Vec::new();
        let mut s = Stitcher::new(plan_of(&[&a]), &mut out);
        s.begin_segment(a.table.clone())?;
        s.feed(bytes, &mut out)?;
        s.end_segment()?;
        s.finish(&mut out).map(|_| ())
    };
    run(&a.bytes).unwrap();
    // A short read inside the size the `packs` row states.
    let err = run(&a.bytes[..a.bytes.len() - 1]).unwrap_err();
    assert!(is_corrupt(&err), "{err}");
    let w = aspen_core::error::ToWire::to_wire(&err);
    assert!(w.alert && w.retryable, "corruption pages");
    let mut long = a.bytes.clone();
    long.push(0);
    assert!(is_corrupt(&run(&long).unwrap_err()));
    // Flipped bytes in data, a header, the count, and the trailer.
    for at in [a.bytes.len() / 2, 12, 11, a.bytes.len() - 1] {
        let mut bad = a.bytes.clone();
        bad[at] ^= 0x01;
        assert!(is_corrupt(&run(&bad).unwrap_err()), "byte {at}");
    }
}

#[test]
fn the_plan_and_the_segments_must_agree() {
    let dir = scratch();
    let mut rng = Rng(5);
    let uni = universe(&mut rng, 8);
    let a = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let b = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let mut out = Vec::new();
    let mut s = Stitcher::new(plan_of(&[&a]), &mut out);
    assert!(is_corrupt(&s.begin_segment(b.table.clone()).unwrap_err()));
    // Finishing early.
    let s = Stitcher::new(plan_of(&[&a]), &mut out);
    assert!(is_corrupt(&s.finish(&mut out).unwrap_err()));
    // Unsorted ID lists.
    let ids = vec![
        (ObjectId::from_bytes([2; 20]), 12),
        (ObjectId::from_bytes([1; 20]), 40),
    ];
    let err = StitchPlan::new(vec![SegmentIds {
        pack: pack_id(1),
        checksum: *a.table.checksum(),
        ids: ids.into_iter(),
    }])
    .unwrap_err();
    assert!(matches!(err, StoreError::Corrupt { .. }));
}

#[test]
fn stitching_real_git_packs_with_overlapping_covers() {
    // Segments cut from a real history the way covers overlap: A = C1..C3,
    // B = C1..C5 (overlaps A), C = the whole history. Any order stitches
    // to a pack stock git accepts with --strict (it is closed).
    let dir = scratch();
    let src = crate::tests::ingest::history(dir.path(), 6);
    let pack = |revs: &str| {
        let out = crate::testutil::git_raw(
            &src,
            &[
                "pack-objects",
                "-q",
                "--revs",
                "--stdout",
                "--delta-base-offset",
            ],
            Some(revs.as_bytes()),
        );
        Seg::new(out.stdout, dir.path())
    };
    let a = pack("HEAD~3\n");
    let b = pack("HEAD~1\n");
    let c = pack("HEAD\n");
    for plan in [
        vec![&a, &b, &c],
        vec![&c, &a],
        vec![&b, &a, &c, &a],
        vec![&a, &c, &b],
    ] {
        let (out, outcome) = stitch(&plan).unwrap();
        check_output(&out, &plan, dir.path());
        assert!(outcome.dropped > 0);
        git_index_pack(&out, dir.path(), true).unwrap();
        if let Some(old) = old_git() {
            git_index_with(&old, &out, dir.path()).unwrap();
        }
    }
}

/// A pack that is a chain of `depth` deltas, starting from a full copy of
/// `start` (itself the end of the previous segment's chain).
fn chain_segment(objs: &[Obj]) -> Vec<u8> {
    let mut specs = vec![Spec::Full(objs[0].clone())];
    for (i, o) in objs.iter().enumerate().skip(1) {
        specs.push(Spec::Ofs(i - 1, o.clone()));
    }
    build_pack(&specs)
}

/// Builds `segments` segments, each a chain of `per_segment` deltas whose
/// first, full object is the previous segment's last object. Keep-first
/// drops each segment's full copy, so the stitched chain is the sum.
fn deep_plan(segments: usize, per_segment: usize, dir: &Path) -> Vec<Seg> {
    let objs: Vec<Obj> = (0..=segments * per_segment)
        .map(|i| Obj::blob(format!("{}{i}\n", "base line\n".repeat(20))))
        .collect();
    (0..segments)
        .map(|s| {
            let slice = &objs[s * per_segment..=(s + 1) * per_segment];
            Seg::new(chain_segment(slice), dir)
        })
        .collect()
}

#[test]
fn keep_first_chains_add_up_across_segments() {
    // S1 item 7: depth approaches the sum of the segments' depths. Here 8
    // segments of depth 50 (git's default pack.depth) stitch to depth 400,
    // which stock git accepts.
    let dir = scratch();
    let segs = deep_plan(8, 50, dir.path());
    let refs: Vec<&Seg> = segs.iter().collect();
    let (out, outcome) = stitch(&refs).unwrap();
    assert_eq!(outcome.max_delta_depth, 400);
    assert_eq!(check_output(&out, &refs, dir.path()), 400);
    git_index_pack(&out, dir.path(), true).unwrap();
    if let Some(old) = old_git() {
        git_index_with(&old, &out, dir.path()).unwrap();
    }
}

/// An old git to check against, from `ASPEN_OLD_GIT` (a `git` binary, such
/// as a 2.30 build). Unset in required CI; the nightly all-tests job sets it
/// to git 2.30.9 built from testkit/images/git.
fn old_git() -> Option<PathBuf> {
    std::env::var_os("ASPEN_OLD_GIT").map(PathBuf::from)
}

fn git_index_with(git: &Path, pack: &[u8], dir: &Path) -> Result<(), String> {
    let p = dir.join(format!("old-{}.pack", crate::sha1(pack)));
    std::fs::write(&p, pack).unwrap();
    let out = std::process::Command::new(git)
        .args(["index-pack", "--strict"])
        .arg(&p)
        .current_dir(dir)
        .output()
        .unwrap();
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(p.with_extension("idx"));
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).into_owned())
    }
}

/// Indexes `pack` into a fresh bare repository with `git` and reads the
/// last object of the last segment (the end of the longest chain).
fn read_deepest(git: &Path, pack: &[u8], segs: &[Seg], dir: &Path) -> std::time::Duration {
    let repo = dir.join(format!("deep-{}.git", crate::sha1(pack)));
    let run = |args: &[&str], cwd: &Path| {
        let out = std::process::Command::new(git)
            .args(args)
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    };
    run(&["init", "-q", "--bare", repo.to_str().unwrap()], dir);
    let p = repo.join("objects/pack/pack-deep.pack");
    std::fs::write(&p, pack).unwrap();
    run(&["index-pack", p.to_str().unwrap()], &repo);
    let last = segs
        .last()
        .unwrap()
        .table
        .entries()
        .last()
        .unwrap()
        .id
        .to_hex();
    let started = std::time::Instant::now();
    run(&["cat-file", "-p", &last], &repo);
    let t = started.elapsed();
    std::fs::remove_dir_all(&repo).unwrap();
    t
}

/// Measures the deepest stitched chain an old git accepts. Run by hand:
/// `ASPEN_OLD_GIT=/path/to/git-2.30 cargo test -p aspen-pack -- --ignored
/// depth_limit --nocapture`. Without `ASPEN_OLD_GIT` there is nothing to
/// measure against, so it says so and returns instead of failing every
/// `--include-ignored` run on a machine without an old git. A job that must
/// measure (the nightly all-tests job) sets `ASPEN_REQUIRE_OLD_GIT`, and then
/// a missing `ASPEN_OLD_GIT` fails the test instead of skipping it.
#[test]
#[ignore = "measurement; skips without ASPEN_OLD_GIT (required in nightly)"]
fn depth_limit_of_old_git() {
    let Some(git) = old_git() else {
        assert!(
            std::env::var_os("ASPEN_REQUIRE_OLD_GIT").is_none(),
            "depth_limit_of_old_git: ASPEN_REQUIRE_OLD_GIT is set but ASPEN_OLD_GIT is not; \
             the job that requires this measurement must provide an old git binary"
        );
        eprintln!(
            "depth_limit_of_old_git: skipped, ASPEN_OLD_GIT is unset (set it to an old git binary, such as a 2.30 build, to measure)"
        );
        return;
    };
    let dir = scratch();
    for depth in [1_000usize, 4_000, 10_000, 50_000] {
        let segs = deep_plan(depth / 50, 50, dir.path());
        let refs: Vec<&Seg> = segs.iter().collect();
        let (out, outcome) = stitch(&refs).unwrap();
        let started = std::time::Instant::now();
        let r = git_index_with(&git, &out, dir.path());
        let indexed = started.elapsed();
        // Read the deepest object back through the whole chain, as a
        // checkout would.
        let read = r
            .as_ref()
            .ok()
            .map(|()| read_deepest(&git, &out, &segs, dir.path()));
        println!(
            "depth {} ({} segments of 50): {} (index-pack --strict {:.2?}{})",
            outcome.max_delta_depth,
            segs.len(),
            match &r {
                Ok(()) => "accepted".to_owned(),
                Err(e) => format!("REJECTED: {}", e.lines().last().unwrap_or("")),
            },
            indexed,
            read.map(|t| format!(", cat-file of the deepest object {t:.2?}"))
                .unwrap_or_default()
        );
    }
}

#[test]
fn an_entry_table_that_names_entries_unlike_the_idx_is_corrupt() {
    // Invariant: the stitcher drops entries by the IDs in the entry table,
    // which has no checksum; the plan comes from the checksummed `.idx`.
    // A table whose IDs are swapped between two entries (same ID set, same
    // count) must be refused, or the output could hold one object twice and
    // lack another behind a valid count and trailer.
    let dir = scratch();
    let mut rng = Rng(23);
    let uni = universe(&mut rng, 8);
    let a = Seg::new(random_pack(&mut rng, &uni), dir.path());
    let mut entries = a.table.entries().to_vec();
    assert!(entries.len() >= 2);
    let (x, y) = (entries[0].id, entries[1].id);
    entries[0].id = y;
    entries[1].id = x;
    let swapped = EntryTable::new(*a.table.checksum(), a.table.pack_size(), entries).unwrap();
    let mut out = Vec::new();
    let mut s = Stitcher::new(plan_of(&[&a]), &mut out);
    let err = s.begin_segment(swapped).unwrap_err();
    assert!(is_corrupt(&err), "{err}");
    // The true table passes.
    let mut s = Stitcher::new(plan_of(&[&a]), &mut out);
    s.begin_segment(a.table.clone()).unwrap();
}
