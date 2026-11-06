//! The compaction repack writer (node C1): the output holds exactly the
//! union of its sources, first copy wins, is self-contained and base-first,
//! never deeper than the cap, byte-identical across runs, and refuses a
//! source whose entry table, `.idx` and bytes disagree.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use aspen_core::id::{ObjectId, RepoId};
use aspen_core::ingest::ObjectKind;
use aspen_core::pack::{CoverLevel, EntryKind, EntryTable, PackOrigin};

use crate::LayoutError;
use crate::idx::IdxV2;
use crate::repack::{RepackOptions, RepackSource, RepackedPack, write_repack};
use crate::testutil::{
    Base, MAX_INFLATED, MAX_OBJECT, Obj, Spec, build_pack, git_index_pack, git_raw, scratch,
    table_of,
};
use crate::verify::{VerifiedPack, verify_pack};

/// SplitMix64: a printed seed replays exactly.
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

/// A stored pack on disk: pack, `.idx` (written by stock git) and entry
/// table (from `verify_pack`, so the source itself is a valid stored pack).
pub(super) struct Src {
    pub(super) pack: PathBuf,
    pub(super) idx: PathBuf,
    pub(super) table: EntryTable,
}

impl Src {
    pub(super) fn new(bytes: &[u8], dir: &Path) -> Self {
        git_index_pack(bytes, dir, false).unwrap_or_else(|e| panic!("{e}"));
        let pack = dir.join(format!("check-{}.pack", crate::sha1(bytes)));
        let idx = pack.with_extension("idx");
        let table = table_of(bytes, dir);
        Self { pack, idx, table }
    }

    pub(super) fn source(&self) -> RepackSource<'_> {
        RepackSource {
            pack: &self.pack,
            idx: &self.idx,
            table: &self.table,
        }
    }

    /// IDs as the `.idx` lists them.
    pub(super) fn idx_ids(&self) -> BTreeSet<ObjectId> {
        let bytes = std::fs::read(&self.idx).unwrap();
        IdxV2::parse(&bytes, None).unwrap().ids().collect()
    }
}

fn options(cap: u32) -> RepackOptions {
    RepackOptions {
        max_delta_depth: cap,
        max_tree_delta_depth: cap,
        max_object_bytes: MAX_OBJECT,
    }
}

fn repack(srcs: &[&Src], cap: u32, out: &Path) -> Result<RepackedPack, LayoutError> {
    let sources: Vec<RepackSource<'_>> = srcs.iter().map(|s| s.source()).collect();
    write_repack(&sources, out, &options(cap))
}

/// A repack, with every invariant checked independently of the writer.
struct Checked {
    bytes: Vec<u8>,
    pack: RepackedPack,
    verified: VerifiedPack,
}

impl Checked {
    fn kind_of(&self, id: &ObjectId) -> EntryKind {
        self.entry(id).kind
    }
    fn entry(&self, id: &ObjectId) -> aspen_core::pack::PackEntry {
        *self
            .verified
            .table
            .entries()
            .iter()
            .find(|e| e.id == *id)
            .unwrap()
    }
    fn offset_of(&self, id: &ObjectId) -> u64 {
        self.entry(id).offset
    }
}

/// Repacks `srcs` with `cap` and checks: the output passes `verify_pack`
/// (self-contained, base-first, no object twice) and the writer's `.idx`
/// and entry table equal what gix derives from the bytes; its objects are
/// exactly the union of the sources' `.idx` IDs, in first-appearance order
/// (keep-first, order preserved); its depth is at most `cap` and as
/// reported; files are named by the checksum; and the `packs` row is a
/// valid compaction row describing those files.
fn run(srcs: &[&Src], cap: u32, dir: &Path) -> Checked {
    let out = dir.join(format!("out-{}", crate::sha1(format!("{cap}").as_bytes())));
    let pack = repack(srcs, cap, &out).unwrap_or_else(|e| panic!("repack failed: {e}"));
    let bytes = std::fs::read(&pack.pack_path).unwrap();
    let verified = verify_pack(&pack.pack_path, dir, MAX_OBJECT, MAX_INFLATED)
        .unwrap_or_else(|e| panic!("repacked pack breaks an invariant: {e}"));
    assert_eq!(
        std::fs::read(&pack.idx_path).unwrap(),
        verified.idx,
        "the .idx (CRCs included) matches gix's, and so git's"
    );
    assert_eq!(
        std::fs::read(&pack.entries_path).unwrap(),
        verified.table.encode()
    );
    assert!(verified.max_delta_depth <= cap, "depth over the cap");
    assert_eq!(verified.max_delta_depth, pack.max_delta_depth);

    let union: BTreeSet<ObjectId> = srcs.iter().flat_map(|s| s.idx_ids()).collect();
    let got: BTreeSet<ObjectId> = verified.table.entries().iter().map(|e| e.id).collect();
    assert_eq!(got, union, "exactly the union of the sources");
    let mut seen = BTreeSet::new();
    let first_order: Vec<ObjectId> = srcs
        .iter()
        .flat_map(|s| s.table.entries().iter().map(|e| e.id))
        .filter(|id| seen.insert(*id))
        .collect();
    let out_order: Vec<ObjectId> = verified.table.entries().iter().map(|e| e.id).collect();
    assert_eq!(out_order, first_order, "first copies, in source order");
    let total: usize = srcs.iter().map(|s| s.table.entries().len()).sum();
    assert_eq!(pack.dropped, (total - union.len()) as u64);
    assert_eq!(pack.object_count as usize, union.len());

    assert_eq!(&bytes[bytes.len() - 20..], pack.checksum.as_bytes());
    let hex = pack.checksum.to_hex();
    assert_eq!(pack.pack_path, out.join(format!("{hex}.pack")));
    assert_eq!(pack.idx_path, out.join(format!("{hex}.idx")));
    assert_eq!(pack.entries_path, out.join(format!("{hex}.entries")));
    let level = CoverLevel::new(2).unwrap();
    let row = pack.new_pack(RepoId::new(1).unwrap(), level);
    row.validate().unwrap();
    assert_eq!(row.origin, PackOrigin::Compaction { level });
    assert_eq!(row.size, bytes.len() as u64);
    assert_eq!(
        row.idx_size,
        std::fs::metadata(&pack.idx_path).unwrap().len()
    );
    assert_eq!(
        row.entries_size,
        std::fs::metadata(&pack.entries_path).unwrap().len()
    );
    assert_eq!(row.ref_delta_count, verified.table.ref_delta_count());
    Checked {
        bytes,
        pack,
        verified,
    }
}

fn big(tag: &str) -> Vec<u8> {
    (0..300)
        .map(|i| format!("{tag} {i}\n"))
        .collect::<String>()
        .into_bytes()
}

fn grown(base: &Obj, tail: &str) -> Obj {
    let mut d = base.data.clone();
    d.extend_from_slice(tail.as_bytes());
    Obj::blob(d)
}

/// Invariant: keep-first. An object held by two sources is emitted once,
/// as the first source stores it; a later source's delta whose base copy
/// was dropped hangs off the earlier source's copy (OFS_DELTA distance
/// rewritten, REF_DELTA unchanged), and its depth adds up.
#[test]
fn the_first_copy_wins_and_later_deltas_hang_off_it() {
    let dir = scratch();
    let a = Obj::blob(big("a"));
    let b = grown(&a, "b\n");
    let c = grown(&b, "c\n");
    let d = grown(&b, "d\n");
    // Source 1 stores b as a delta on a; source 2 stores b whole, with c
    // (OFS_DELTA) and d (REF_DELTA) on it.
    let s1 = Src::new(
        &build_pack(&[Spec::Full(a.clone()), Spec::Ofs(0, b.clone())]),
        dir.path(),
    );
    let s2 = Src::new(
        &build_pack(&[
            Spec::Full(b.clone()),
            Spec::Ofs(0, c.clone()),
            Spec::Ref(Base::Entry(0), d.clone()),
        ]),
        dir.path(),
    );
    let out = run(&[&s1, &s2], 50, dir.path());
    assert_eq!(out.pack.dropped, 1);
    assert_eq!(out.pack.undeltified, 0);
    assert_eq!(
        out.kind_of(&b.id()),
        EntryKind::OfsDelta {
            base_offset: out.offset_of(&a.id())
        },
        "b is source 1's copy"
    );
    assert_eq!(
        out.kind_of(&c.id()),
        EntryKind::OfsDelta {
            base_offset: out.offset_of(&b.id())
        }
    );
    assert_eq!(out.kind_of(&d.id()), EntryKind::RefDelta { base: b.id() });
    assert_eq!(out.pack.max_delta_depth, 2, "a <- b <- c");
    assert_eq!(out.pack.ref_delta_count, 1);
    git_index_pack(&out.bytes, dir.path(), true).unwrap();

    // The other order keeps source 2's whole copy of b instead, and
    // source 1's b is dropped although it is the only delta on a.
    let out = run(&[&s2, &s1], 50, dir.path());
    assert_eq!(out.pack.dropped, 1);
    assert_eq!(out.kind_of(&b.id()), EntryKind::Blob);
    assert_eq!(out.pack.max_delta_depth, 1);
}

/// Invariant: stock git packs of overlapping history (the way covers
/// overlap) repack to the union, which stock git accepts with `--strict`
/// because the union of closed packs is closed.
#[test]
fn overlapping_git_packs_repack_to_a_pack_git_accepts() {
    let dir = scratch();
    let src = crate::tests::ingest::history(dir.path(), 8);
    let pack = |revs: &str, ofs: bool| {
        let mut args = vec!["pack-objects", "-q", "--revs", "--stdout"];
        if ofs {
            args.push("--delta-base-offset");
        }
        let out = git_raw(&src, &args, Some(revs.as_bytes()));
        assert!(out.status.success());
        Src::new(&out.stdout, dir.path())
    };
    let a = pack("HEAD~5\n", true);
    let b = pack("HEAD~2\n", false);
    let c = pack("HEAD\n^HEAD~4\n", true);
    for plan in [vec![&a, &b, &c], vec![&c, &b, &a], vec![&b, &c]] {
        let out = run(&plan, 50, dir.path());
        assert!(out.pack.dropped > 0);
        let union: BTreeSet<String> = plan
            .iter()
            .flat_map(|s| s.idx_ids())
            .map(|id| id.to_hex())
            .collect();
        let listed = git_index_pack(&out.bytes, dir.path(), true).unwrap();
        assert_eq!(listed.into_iter().collect::<BTreeSet<_>>(), union);
    }
}

/// Builds `segments` packs, each a chain of `per` deltas whose first, full
/// object is the previous pack's last object, so keep-first chains them
/// into one chain of `segments × per`.
fn chained_sources(segments: usize, per: usize, dir: &Path) -> Vec<Src> {
    let objs: Vec<Obj> = (0..=segments * per)
        .map(|i| Obj::blob(format!("{}{i}\n", "base line\n".repeat(20))))
        .collect();
    (0..segments)
        .map(|s| {
            let slice = &objs[s * per..=(s + 1) * per];
            let mut specs = vec![Spec::Full(slice[0].clone())];
            for (i, o) in slice.iter().enumerate().skip(1) {
                specs.push(Spec::Ofs(i - 1, o.clone()));
            }
            Src::new(&build_pack(&specs), dir)
        })
        .collect()
}

/// Invariant: keep-first adds depths across sources, and the cap still
/// holds. 4 sources of depth 30 chain to depth 120; with a cap of 50 the
/// entries at chain positions 51 and 102 are stored whole and every depth
/// is its position modulo 51. Every object still reads back (verify_pack
/// resolves every object, and so does stock git).
#[test]
fn the_depth_cap_breaks_chains_that_add_up_across_sources() {
    let dir = scratch();
    let srcs = chained_sources(4, 30, dir.path());
    let refs: Vec<&Src> = srcs.iter().collect();
    let uncapped = run(&refs, 1000, dir.path());
    assert_eq!(uncapped.pack.max_delta_depth, 120);
    assert_eq!(uncapped.pack.undeltified, 0);
    let capped = run(&refs, 50, dir.path());
    assert_eq!(capped.pack.max_delta_depth, 50);
    assert_eq!(capped.pack.undeltified, 2);
    let full: Vec<usize> = capped
        .verified
        .table
        .entries()
        .iter()
        .enumerate()
        .filter(|(_, e)| e.kind == EntryKind::Blob)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(full, [0, 51, 102]);
    git_index_pack(&capped.bytes, dir.path(), true).unwrap();
    // A cap of 0 stores everything whole.
    let flat = run(&refs, 0, dir.path());
    assert_eq!(flat.pack.max_delta_depth, 0);
    assert_eq!(flat.pack.undeltified, 120);
}

/// Invariant (item 11): trees have their own, lower cap. Sources whose
/// tree chains and blob chains add up to depth 120 across sources repack to
/// trees at most `max_tree_delta_depth` deep and blobs at most
/// `max_delta_depth`, as both the writer and an independent depth count over
/// the output say; every object still reads back (`verify_pack` and stock
/// git resolve each one).
#[test]
fn trees_are_capped_lower_than_blobs() {
    let dir = scratch();
    let (segments, per) = (4, 30);
    let chain = |kind: ObjectKind, tag: &str| -> Vec<Obj> {
        (0..=segments * per)
            .map(|i| Obj {
                kind,
                data: format!("{}{i}\n", format!("{tag} line\n").repeat(20)).into_bytes(),
            })
            .collect()
    };
    let (trees, blobs) = (
        chain(ObjectKind::Tree, "tree"),
        chain(ObjectKind::Blob, "blob"),
    );
    let srcs: Vec<Src> = (0..segments)
        .map(|s| {
            let mut specs = Vec::new();
            for objs in [&trees, &blobs] {
                let at = specs.len();
                let slice = &objs[s * per..=(s + 1) * per];
                specs.push(Spec::Full(slice[0].clone()));
                for (i, o) in slice.iter().enumerate().skip(1) {
                    specs.push(Spec::Ofs(at + i - 1, o.clone()));
                }
            }
            Src::new(&build_pack(&specs), dir.path())
        })
        .collect();
    let refs: Vec<&Src> = srcs.iter().collect();
    let sources: Vec<RepackSource<'_>> = refs.iter().map(|s| s.source()).collect();
    let out = dir.path().join("tree-capped");
    let pack = write_repack(
        &sources,
        &out,
        &RepackOptions {
            max_delta_depth: 50,
            max_tree_delta_depth: 10,
            max_object_bytes: MAX_OBJECT,
        },
    )
    .unwrap();
    let verified = verify_pack(&pack.pack_path, dir.path(), MAX_OBJECT, MAX_INFLATED).unwrap();
    assert_eq!((pack.max_delta_depth, pack.max_tree_delta_depth), (50, 10));
    assert_eq!(
        (verified.max_delta_depth, verified.max_tree_delta_depth),
        (50, 10)
    );
    // Blob chain of 120: whole at 0, 51, 102. Tree chain of 120: whole
    // every 11th.
    assert_eq!(pack.undeltified, 2 + 120 / 11);
    // Not `--strict`: the made-up trees do not parse as trees.
    git_index_pack(&std::fs::read(&pack.pack_path).unwrap(), dir.path(), false).unwrap();
    // The default compaction setting for blobs is unchanged by the tree
    // cap: with both at 50 the trees go as deep as the blobs.
    let both = run(&refs, 50, dir.path());
    assert_eq!(both.verified.max_tree_delta_depth, 50);
}

/// Invariant: the cap holds for a deep chain inside one stock git pack
/// (`pack-objects --no-reuse-delta --depth=200 --window=250`; a client may
/// send up to 4095), and every undeltified object is the object it
/// replaced (stock git reads every object back from the repacked pack
/// alone, identical to the source repository's).
#[test]
fn a_deep_stock_git_pack_is_capped() {
    let dir = scratch();
    let repo = dir.path().join("deep");
    std::fs::create_dir_all(&repo).unwrap();
    crate::testutil::git(&repo, &["init", "-q", "--bare"]);
    // 300 commits each editing one line of a file in place, via
    // fast-import: each version is closest to its neighbours, so git
    // chains them (measured: depth about 197).
    let mut stream = String::new();
    let mut lines: Vec<String> = (0..400).map(|j| format!("line {j}\n")).collect();
    for i in 1..=300 {
        lines[(i * 37) % 400] = format!("edited in commit {i}\n");
        let text = lines.concat();
        stream.push_str(&format!(
            "commit refs/heads/main\ncommitter A <a@example.invalid> {i} +0000\ndata 3\nc{}\n",
            i % 10
        ));
        stream.push_str(&format!(
            "M 100644 inline f.txt\ndata {}\n{text}\n",
            text.len()
        ));
    }
    let out = git_raw(&repo, &["fast-import", "--quiet"], Some(stream.as_bytes()));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = git_raw(
        &repo,
        &[
            "pack-objects",
            "-q",
            "--revs",
            "--stdout",
            "--delta-base-offset",
            "--no-reuse-delta",
            "--depth=200",
            "--window=250",
        ],
        Some(b"refs/heads/main\n"),
    );
    assert!(out.status.success());
    let src = Src::new(&out.stdout, dir.path());
    let source_depth = crate::verify::max_delta_depth(&src.table);
    assert!(source_depth > 150, "the source is {source_depth} deep");
    let capped = run(&[&src], 50, dir.path());
    assert!(capped.pack.undeltified > 0);
    git_index_pack(&capped.bytes, dir.path(), true).unwrap();
    // Read every object back through stock git from the repacked pack
    // alone, and compare with the source repository (which holds exactly
    // the source pack's objects).
    let check = dir.path().join("check.git");
    crate::testutil::git(dir.path(), &["init", "-q", "--bare", "check.git"]);
    let packdir = check.join("objects/pack");
    std::fs::copy(&capped.pack.pack_path, packdir.join("p.pack")).unwrap();
    std::fs::copy(&capped.pack.idx_path, packdir.join("p.idx")).unwrap();
    let all = ["cat-file", "--batch-all-objects", "--batch"];
    let ours = git_raw(&check, &all, None);
    let theirs = git_raw(&repo, &all, None);
    assert!(ours.status.success() && theirs.status.success());
    assert_eq!(ours.stdout.len(), theirs.stdout.len());
    assert!(ours.stdout == theirs.stdout, "every object reads back");
}

/// Invariant: determinism. The same sources in the same order give a
/// byte-identical pack, `.idx` and entry table, recompressed entries
/// included.
#[test]
fn repacking_twice_gives_identical_bytes() {
    let dir = scratch();
    let srcs = chained_sources(3, 20, dir.path());
    let refs: Vec<&Src> = srcs.iter().collect();
    let one = repack(&refs, 7, &dir.path().join("one")).unwrap();
    let two = repack(&refs, 7, &dir.path().join("two")).unwrap();
    assert!(one.undeltified > 0);
    assert_eq!(one.checksum, two.checksum);
    for (a, b) in [
        (&one.pack_path, &two.pack_path),
        (&one.idx_path, &two.idx_path),
        (&one.entries_path, &two.entries_path),
    ] {
        assert_eq!(std::fs::read(a).unwrap(), std::fs::read(b).unwrap());
    }
    // Writing into the same directory again is idempotent.
    let again = repack(&refs, 7, &dir.path().join("one")).unwrap();
    assert_eq!(again, one);
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

/// A random stored-pack layout over a random subset of `universe`:
/// self-contained, base-first, no repeats, mixing full entries, OFS_DELTAs
/// and REF_DELTAs, often in long chains.
fn random_pack(rng: &mut Rng, universe: &[Obj]) -> Vec<u8> {
    let mut picked: Vec<usize> = (0..universe.len()).filter(|_| rng.below(3) != 0).collect();
    if picked.is_empty() {
        picked.push(rng.below(universe.len()));
    }
    for i in (1..picked.len()).rev() {
        picked.swap(i, rng.below(i + 1));
    }
    let mut specs = Vec::new();
    for (pos, &u) in picked.iter().enumerate() {
        let o = universe[u].clone();
        // Bias towards the previous entry to build deep chains.
        let base = if rng.below(2) == 0 {
            pos.saturating_sub(1)
        } else {
            rng.below(pos.max(1))
        };
        specs.push(match (pos, rng.below(4)) {
            (0, _) | (_, 0) => Spec::Full(o),
            (_, 1) => Spec::Ref(Base::Entry(base), o),
            _ => Spec::Ofs(base, o),
        });
    }
    build_pack(&specs)
}

/// Invariant: every property above, over many random plans: 1 to 5
/// sources (repeats allowed) from a pool of random packs over overlapping
/// object sets, with caps from 0 to unbounded.
#[test]
fn random_overlapping_sources_repack_to_valid_packs() {
    let dir = scratch();
    for seed in 0..120u64 {
        let mut rng = Rng(0xc1_0000 + seed);
        let uni = universe(&mut rng, 16);
        let pool: Vec<Src> = (0..4)
            .map(|_| Src::new(&random_pack(&mut rng, &uni), dir.path()))
            .collect();
        let plan: Vec<&Src> = (0..1 + rng.below(5))
            .map(|_| &pool[rng.below(pool.len())])
            .collect();
        let cap = [0, 1, 2, 3, 5, 4095][rng.below(6)];
        let out = run(&plan, cap, dir.path());
        // Blobs reference nothing, so any union is closed for --strict.
        if seed % 10 == 0 {
            git_index_pack(&out.bytes, dir.path(), true)
                .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        }
        for s in pool {
            let _ = std::fs::remove_file(&s.pack);
            let _ = std::fs::remove_file(&s.idx);
        }
    }
}

fn is_mismatch(e: &LayoutError) -> bool {
    matches!(
        e,
        LayoutError::Mismatch(_) | LayoutError::Table(_) | LayoutError::Header { .. }
    )
}

/// Invariant: entry tables carry no checksum, so each source's table is
/// checked against its `.idx` before anything is written. A table whose IDs
/// are swapped between two entries (same set, same count), a table of a
/// different pack, a table that says an entry is a different kind, and one
/// that names a delta's base wrongly are all refused, and no output is
/// left behind.
#[test]
fn a_source_whose_entry_table_disagrees_with_its_idx_is_refused() {
    let dir = scratch();
    let a = Obj::blob(big("a"));
    let b = grown(&a, "b\n");
    let c = Obj::blob(big("c"));
    let s = Src::new(
        &build_pack(&[
            Spec::Full(a.clone()),
            Spec::Full(c.clone()),
            Spec::Ofs(0, b.clone()),
        ]),
        dir.path(),
    );
    let other = Src::new(&build_pack(&[Spec::Full(c.clone())]), dir.path());
    let out = dir.path().join("refused");
    let refused = |table: &EntryTable| {
        let src = RepackSource {
            pack: &s.pack,
            idx: &s.idx,
            table,
        };
        let e = write_repack(&[other.source(), src], &out, &options(50)).unwrap_err();
        assert!(is_mismatch(&e), "{e}");
        let left: Vec<_> = std::fs::read_dir(&out)
            .map(|d| d.collect())
            .unwrap_or_default();
        assert!(left.is_empty(), "nothing is written");
    };

    let mut swapped = s.table.entries().to_vec();
    let (x, y) = (swapped[0].id, swapped[1].id);
    swapped[0].id = y;
    swapped[1].id = x;
    refused(&EntryTable::new(*s.table.checksum(), s.table.pack_size(), swapped).unwrap());

    refused(&other.table);
    let foreign = EntryTable::new(
        *other.table.checksum(),
        s.table.pack_size(),
        s.table.entries().to_vec(),
    )
    .unwrap();
    refused(&foreign);

    // Same IDs and offsets as the .idx, but the delta is called a blob.
    let mut kinds = s.table.entries().to_vec();
    kinds[2].kind = EntryKind::Blob;
    kinds[2].header_len = 2;
    refused(&EntryTable::new(*s.table.checksum(), s.table.pack_size(), kinds).unwrap());

    // Same IDs and offsets as the .idx, but the delta's base is named as
    // the other full entry: only the entry header check catches it, and
    // without it the delta would be emitted against the wrong base.
    let mut bases = s.table.entries().to_vec();
    bases[2].kind = EntryKind::OfsDelta {
        base_offset: bases[1].offset,
    };
    refused(&EntryTable::new(*s.table.checksum(), s.table.pack_size(), bases).unwrap());

    // The true table passes.
    run(&[&other, &s], 50, dir.path());
}

/// Invariant: a source's bytes are hashed as they are read, so a corrupt
/// byte in an entry that is copied unchanged is refused instead of being
/// copied into a pack with a fresh, valid trailer.
#[test]
fn a_corrupt_source_is_refused() {
    let dir = scratch();
    let a = Obj::blob(big("a"));
    let b = grown(&a, "b\n");
    let bytes = build_pack(&[Spec::Full(a), Spec::Ofs(0, b)]);
    let s = Src::new(&bytes, dir.path());
    let mut bad = bytes.clone();
    // Inside the last entry's zlib stream, past its header.
    let at = bytes.len() - 20 - 2;
    assert!(at > s.table.entries()[1].offset as usize + 8);
    bad[at] ^= 0x01;
    std::fs::write(&s.pack, &bad).unwrap();
    let e = repack(&[&s], 50, &dir.path().join("out")).unwrap_err();
    assert!(is_mismatch(&e), "{e}");
}

#[test]
fn a_repack_needs_an_object() {
    let dir = scratch();
    let e = write_repack(&[], &dir.path().join("out"), &options(50)).unwrap_err();
    assert!(is_mismatch(&e), "{e}");
}
