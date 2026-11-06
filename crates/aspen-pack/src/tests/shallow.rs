//! Exact shallow packs (node C8): the object set [`shallow_objects`]
//! computes equals what stock `git clone --depth` receives, and the subset
//! repack of it ([`write_repack_subset`]) is a valid stored pack that stock
//! git accepts as a shallow repository. A kept delta whose base is left
//! out is stored whole.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use aspen_core::id::ObjectId;
use aspen_core::pack::EntryKind;

use super::repack::Src;
use crate::LayoutError;
use crate::repack::{RepackOptions, RepackSource, RepackedPack, write_repack, write_repack_subset};
use crate::shallow::{MAX_PEELED_TAGS, ShallowRoots, shallow_objects};
use crate::testutil::{
    Base, MAX_INFLATED, MAX_OBJECT, Obj, Spec, build_pack, git, git_index_pack, git_raw, scratch,
};
use crate::verify::verify_pack;

fn id(hex: &str) -> ObjectId {
    ObjectId::parse_hex(hex.trim()).unwrap_or_else(|e| panic!("not an ID: {hex:?}: {e}"))
}

fn ids(lines: &str) -> Vec<ObjectId> {
    lines
        .lines()
        .map(|l| id(l.split_whitespace().next().unwrap()))
        .collect()
}

fn options(cap: u32) -> RepackOptions {
    RepackOptions {
        max_delta_depth: cap,
        max_tree_delta_depth: cap,
        max_object_bytes: MAX_OBJECT,
    }
}

fn sources<'a>(srcs: &[&'a Src]) -> Vec<RepackSource<'a>> {
    srcs.iter().map(|s| s.source()).collect()
}

/// Version `n` of a file that shrinks as `n` grows. Stock git deltifies a
/// smaller version against a larger one, so later versions are stored as
/// deltas of earlier ones, whose bases a shallow pack leaves out.
fn big(n: u32) -> String {
    (0..400 - 30 * n)
        .map(|j| format!("line {j} {}\n", if j % 7 == 0 { n } else { 0 }))
        .collect()
}

/// A history with a rename, an old blob and an old file restored, a side
/// branch merged back, nested directories, a gitlink and two annotated
/// tags (`v2` is a tag of `v1`), all at fixed dates:
///
/// ```text
/// c1 - c2 - c3 - c4 - merge - c6   <- main, v1, v2
///             \       /
///              s1 ----
/// ```
///
/// - c1: `dir/a.txt` (A1), `dir/sub/b.txt` (B1), `top.txt` (T1), `big.txt`
/// - c2: `dir/a.txt` becomes A2
/// - c3: `top.txt` renamed to `dir/moved.txt`
/// - s1 (on c3): `dir/sub/b.txt` becomes B2, adds `side.txt`
/// - c4: `top.txt` restored (T1 again) and `dir/a.txt` back to A1, both
///   blobs from c1
/// - merge of s1
/// - c6: adds the gitlink `mod` (a commit of another repository)
///
/// `big.txt` changes and shrinks in every main commit, so stock git stores
/// its later versions as deltas of earlier ones.
fn shallow_history(dir: &Path) -> PathBuf {
    let src = dir.join("src");
    std::fs::create_dir_all(src.join("dir/sub")).unwrap();
    let write = |path: &str, text: &str| std::fs::write(src.join(path), text).unwrap();
    git(&src, &["init", "-q", "-b", "main"]);
    write("dir/a.txt", "A1\n");
    write("dir/sub/b.txt", "B1\n");
    write("top.txt", "T1\n");
    write("big.txt", &big(1));
    git(&src, &["add", "-A"]);
    git(&src, &["commit", "-q", "-m", "c1"]);
    write("dir/a.txt", "A2\n");
    write("big.txt", &big(2));
    git(&src, &["commit", "-q", "-a", "-m", "c2"]);
    git(&src, &["mv", "top.txt", "dir/moved.txt"]);
    write("big.txt", &big(3));
    git(&src, &["commit", "-q", "-a", "-m", "c3"]);
    git(&src, &["checkout", "-q", "-b", "side"]);
    write("dir/sub/b.txt", "B2\n");
    write("side.txt", "S\n");
    git(&src, &["add", "-A"]);
    git(&src, &["commit", "-q", "-m", "s1"]);
    git(&src, &["checkout", "-q", "main"]);
    write("top.txt", "T1\n");
    write("dir/a.txt", "A1\n");
    write("big.txt", &big(4));
    git(&src, &["add", "-A"]);
    git(&src, &["commit", "-q", "-m", "c4"]);
    git(&src, &["merge", "-q", "--no-edit", "side"]);
    git(
        &src,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,1234567890123456789012345678901234567890,mod",
        ],
    );
    write("big.txt", &big(6));
    git(&src, &["add", "big.txt"]);
    git(&src, &["commit", "-q", "-m", "c6"]);
    git(&src, &["tag", "-a", "v1", "-m", "v1"]);
    git(&src, &["tag", "-a", "v2", "v1", "-m", "v2"]);
    src
}

/// A stored pack of `revs` (`pack-objects --revs` input) from `repo`.
fn stored(repo: &Path, revs: &str, ofs: bool, dir: &Path) -> Src {
    let mut args = vec!["pack-objects", "-q", "--revs", "--stdout"];
    if ofs {
        args.push("--delta-base-offset");
    }
    let out = git_raw(repo, &args, Some(revs.as_bytes()));
    assert!(out.status.success());
    Src::new(&out.stdout, dir)
}

/// Three overlapping stored packs of `shallow_history`, mixing OFS_DELTA
/// and REF_DELTA, whose union is the whole history: the first two
/// parents' closures and the tags with what is new since `main~2`.
fn split_sources(src: &Path, dir: &Path) -> Vec<Src> {
    vec![
        stored(src, "main~1\n", true, dir),
        stored(src, "refs/tags/v2\n^main~2\n", false, dir),
        stored(src, "side\n", true, dir),
    ]
}

/// What stock git sends for `git clone --depth <depth> --branch v1`: the
/// wants (the clone's refs: `v1`, and `v2` by tag following), the region
/// (`rev-list --all`), the object set (`rev-list --objects --all`) and the
/// shallow file.
struct Oracle {
    roots: ShallowRoots,
    objects: HashSet<ObjectId>,
    shallow: String,
    tip: String,
}

fn oracle(src: &Path, depth: u32, dir: &Path) -> Oracle {
    let clone = dir.join(format!("clone-{depth}"));
    let url = format!("file://{}", src.display());
    git(
        dir,
        &[
            "clone",
            "-q",
            "--depth",
            &depth.to_string(),
            "--no-local",
            "--branch",
            "v1",
            &url,
            clone.to_str().unwrap(),
        ],
    );
    let wants = ids(&git(&clone, &["for-each-ref", "--format=%(objectname)"]));
    assert_eq!(wants.len(), 2, "v1 and v2");
    Oracle {
        roots: ShallowRoots {
            wants,
            commits: ids(&git(&clone, &["rev-list", "--all"])),
        },
        objects: ids(&git(&clone, &["rev-list", "--objects", "--all"]))
            .into_iter()
            .collect(),
        shallow: std::fs::read_to_string(clone.join(".git/shallow")).unwrap(),
        tip: git(&clone, &["rev-parse", "HEAD"]).trim().to_owned(),
    }
}

fn hexes(set: &HashSet<ObjectId>) -> BTreeSet<String> {
    set.iter().map(|i| i.to_hex()).collect()
}

/// Invariant: the object set is exactly what stock git sends a shallow
/// clone, at depth 1 (the tip only), 2 (the merge, whose tree holds the
/// side branch's files) and 3 (both of the merge's parents: the restored
/// old blobs of c4 and the side commit), with a nested tag want and a
/// gitlink, from three overlapping packs in either order.
#[test]
fn the_shallow_set_equals_what_git_sends() {
    let dir = scratch();
    let src = shallow_history(dir.path());
    let srcs = split_sources(&src, dir.path());
    let mut sizes = Vec::new();
    for depth in 1..=3 {
        let want = oracle(&src, depth, dir.path());
        for order in [[0, 1, 2], [2, 1, 0]] {
            let plan: Vec<&Src> = order.iter().map(|&i| &srcs[i]).collect();
            let got = shallow_objects(&sources(&plan), &want.roots, MAX_OBJECT, 1000)
                .unwrap_or_else(|e| panic!("depth {depth}: {e}"));
            assert_eq!(hexes(&got), hexes(&want.objects), "depth {depth}");
        }
        sizes.push((want.roots.commits.len(), want.objects.len()));
    }
    // The depths differ, so each one checks a different region.
    assert_eq!(sizes, [(1, 11), (2, 14), (4, 24)]);
}

/// Invariant: the subset repack of the shallow set is a valid stored pack
/// (`verify_pack`: self-contained, base-first, no object twice) holding
/// exactly that set, within the depth cap, and stock git accepts it as a
/// shallow repository: `index-pack --strict`, then `fsck --strict` with
/// the clone's shallow file and refs pointing at the wants. Some kept
/// deltas' bases are left out, so this covers deltas stored whole too.
#[test]
fn the_shallow_pack_is_a_shallow_repository_git_accepts() {
    let dir = scratch();
    let src = shallow_history(dir.path());
    let srcs = split_sources(&src, dir.path());
    let plan: Vec<&Src> = srcs.iter().collect();
    let total: u64 = srcs.iter().map(|s| s.table.entries().len() as u64).sum();
    let mut base_left_out = 0;
    for depth in 1..=3 {
        let want = oracle(&src, depth, dir.path());
        for cap in [0, 1, 50] {
            let keep = shallow_objects(&sources(&plan), &want.roots, MAX_OBJECT, 1000).unwrap();
            let out = dir.path().join(format!("shallow-{depth}-{cap}"));
            let pack = write_repack_subset(&sources(&plan), &keep, &out, &options(cap))
                .unwrap_or_else(|e| panic!("depth {depth}: {e}"));
            check_subset(&pack, &keep, cap, total, dir.path());
            base_left_out += pack.base_left_out;

            // Stock git's own shallow fetch: write the boundary, then index
            // the pack with --strict in the repository (git 2.39 crashes on
            // --strict outside one when links leave the pack), then point
            // refs at the wants.
            let repo = dir.path().join(format!("check-{depth}-{cap}.git"));
            git(
                dir.path(),
                &["init", "-q", "--bare", repo.to_str().unwrap()],
            );
            let packed = repo.join("objects/pack/shallow.pack");
            std::fs::copy(&pack.pack_path, &packed).unwrap();
            let index = ["index-pack", "--strict", packed.to_str().unwrap()];
            let refused = git_raw(&repo, &index, None);
            assert!(
                !refused.status.success(),
                "without the boundary, the parents below it are missing"
            );
            std::fs::write(repo.join("shallow"), &want.shallow).unwrap();
            git(&repo, &index);
            let idx = std::fs::read(packed.with_extension("idx")).unwrap();
            let listed = git_raw(&repo, &["show-index"], Some(&idx));
            let listed: BTreeSet<String> = String::from_utf8(listed.stdout)
                .unwrap()
                .lines()
                .map(|l| l.split_whitespace().nth(1).unwrap().to_owned())
                .collect();
            assert_eq!(listed, hexes(&keep), "git lists exactly the kept set");
            git(&repo, &["update-ref", "refs/heads/main", &want.tip]);
            git(&repo, &["symbolic-ref", "HEAD", "refs/heads/main"]);
            for (i, w) in want.roots.wants.iter().enumerate() {
                git(
                    &repo,
                    &["update-ref", &format!("refs/tags/w{i}"), &w.to_hex()],
                );
            }
            let fsck = git_raw(&repo, &["fsck", "--strict", "--no-dangling"], None);
            assert!(
                fsck.status.success() && fsck.stdout.is_empty(),
                "depth {depth}, cap {cap}: fsck: {}{}",
                String::from_utf8_lossy(&fsck.stdout),
                String::from_utf8_lossy(&fsck.stderr)
            );
            // Without the shallow file the boundary's parents are missing,
            // so fsck checks the boundary too.
            std::fs::remove_file(repo.join("shallow")).unwrap();
            let fsck = git_raw(&repo, &["fsck", "--strict", "--no-dangling"], None);
            assert!(
                !fsck.status.success(),
                "depth {depth}: fsck needs the boundary"
            );
        }
    }
    // `big.txt` shrinks, so its later versions are deltas of earlier ones
    // and some kept delta's base was left out (and stored whole) above.
    assert!(base_left_out > 0);
}

/// Checks a subset repack independently of the writer: `verify_pack`
/// passes and its `.idx` and entry table equal the writer's, the objects
/// are exactly `keep`, the depth is within `cap` and as reported, and every
/// source entry is counted once as emitted, dropped or left out.
fn check_subset(pack: &RepackedPack, keep: &HashSet<ObjectId>, cap: u32, total: u64, dir: &Path) {
    let verified = verify_pack(&pack.pack_path, dir, MAX_OBJECT, MAX_INFLATED)
        .unwrap_or_else(|e| panic!("the subset breaks an invariant: {e}"));
    assert_eq!(std::fs::read(&pack.idx_path).unwrap(), verified.idx);
    assert_eq!(
        std::fs::read(&pack.entries_path).unwrap(),
        verified.table.encode()
    );
    let got: HashSet<ObjectId> = verified.table.entries().iter().map(|e| e.id).collect();
    assert_eq!(hexes(&got), hexes(keep), "exactly the kept set");
    assert_eq!(pack.object_count as usize, keep.len());
    assert!(verified.max_delta_depth <= cap);
    assert_eq!(verified.max_delta_depth, pack.max_delta_depth);
    assert_eq!(
        u64::from(pack.object_count) + pack.dropped + pack.left_out,
        total
    );
    assert!(pack.base_left_out <= pack.undeltified);
}

fn grown(base: &Obj, tail: &str) -> Obj {
    let mut d = base.data.clone();
    d.extend_from_slice(tail.as_bytes());
    Obj::blob(d)
}

fn set(objs: &[&Obj]) -> HashSet<ObjectId> {
    objs.iter().map(|o| o.id()).collect()
}

/// Invariant: a kept delta whose base is left out is stored whole, and
/// counted in `undeltified` and `base_left_out`; a kept delta on a kept
/// base stays a delta (OFS_DELTA or REF_DELTA), with its depth counted
/// from the new chain start. Stock git accepts each subset.
#[test]
fn a_kept_delta_whose_base_is_left_out_is_stored_whole() {
    let dir = scratch();
    let a = Obj::blob(big(1));
    let b = grown(&a, "b\n");
    let c = grown(&b, "c\n");
    let d = grown(&c, "d\n");
    // a <- b (OFS) <- c (REF) <- d (OFS)
    let s = Src::new(
        &build_pack(&[
            Spec::Full(a.clone()),
            Spec::Ofs(0, b.clone()),
            Spec::Ref(Base::Entry(1), c.clone()),
            Spec::Ofs(2, d.clone()),
        ]),
        dir.path(),
    );
    let run = |keep: &HashSet<ObjectId>, name: &str| {
        let pack = write_repack_subset(&sources(&[&s]), keep, &dir.path().join(name), &options(50))
            .unwrap();
        check_subset(&pack, keep, 50, 4, dir.path());
        let bytes = std::fs::read(&pack.pack_path).unwrap();
        git_index_pack(&bytes, dir.path(), true).unwrap();
        let v = verify_pack(&pack.pack_path, dir.path(), MAX_OBJECT, MAX_INFLATED).unwrap();
        let kinds: Vec<EntryKind> = v.table.entries().iter().map(|e| e.kind).collect();
        (pack, kinds)
    };

    // b's base a is left out: b is stored whole; c stays a REF_DELTA on b,
    // d an OFS_DELTA on c.
    let (pack, kinds) = run(&set(&[&b, &c, &d]), "bcd");
    assert_eq!((pack.undeltified, pack.base_left_out), (1, 1));
    assert_eq!((pack.left_out, pack.dropped), (1, 0));
    assert_eq!(kinds[0], EntryKind::Blob);
    assert_eq!(kinds[1], EntryKind::RefDelta { base: b.id() });
    assert!(matches!(kinds[2], EntryKind::OfsDelta { .. }));
    assert_eq!(pack.max_delta_depth, 2);

    // c's base b is left out (a REF_DELTA), and so is d's base c (an
    // OFS_DELTA) once only a and d are kept.
    let (pack, kinds) = run(&set(&[&a, &d]), "ad");
    assert_eq!((pack.undeltified, pack.base_left_out), (1, 1));
    assert_eq!(pack.left_out, 2);
    assert_eq!(kinds, [EntryKind::Blob, EntryKind::Blob]);
    let (pack, kinds) = run(&set(&[&a, &c]), "ac");
    assert_eq!((pack.undeltified, pack.base_left_out), (1, 1));
    assert_eq!(kinds, [EntryKind::Blob, EntryKind::Blob]);

    // Everything kept: nothing is undeltified.
    let (pack, _) = run(&set(&[&a, &b, &c, &d]), "all");
    assert_eq!(
        (pack.undeltified, pack.base_left_out, pack.left_out),
        (0, 0, 0)
    );
    assert_eq!(pack.max_delta_depth, 3);
}

/// Invariant: the depth cap applies to a subset, and is counted apart
/// from deltas whose base was left out.
#[test]
fn the_depth_cap_applies_to_a_subset() {
    let dir = scratch();
    let objs: Vec<Obj> = (0..8)
        .map(|i| Obj::blob(format!("{}{i}\n", "base line\n".repeat(20))))
        .collect();
    let mut specs = vec![Spec::Full(objs[0].clone())];
    for (i, o) in objs.iter().enumerate().skip(1) {
        specs.push(Spec::Ofs(i - 1, o.clone()));
    }
    let s = Src::new(&build_pack(&specs), dir.path());
    // Leave out object 1: object 2 is stored whole (base left out), then
    // 3 to 7 chain on it, and with a cap of 2 objects 5 is stored whole
    // again for depth.
    let keep: HashSet<ObjectId> = objs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 1)
        .map(|(_, o)| o.id())
        .collect();
    let pack =
        write_repack_subset(&sources(&[&s]), &keep, &dir.path().join("out"), &options(2)).unwrap();
    check_subset(&pack, &keep, 2, 8, dir.path());
    assert_eq!((pack.undeltified, pack.base_left_out), (2, 1));
    assert_eq!(pack.max_delta_depth, 2);
}

/// Invariant: the subset writer keeping every object writes exactly the
/// bytes `write_repack` writes, and a subset is byte-identical across
/// runs.
#[test]
fn keeping_everything_is_write_repack_and_subsets_are_deterministic() {
    let dir = scratch();
    let src = shallow_history(dir.path());
    let srcs = split_sources(&src, dir.path());
    let plan = sources(&srcs.iter().collect::<Vec<_>>());
    let all: HashSet<ObjectId> = srcs.iter().flat_map(|s| s.idx_ids()).collect();
    for cap in [0, 1, 50] {
        let full = write_repack(
            &plan,
            &dir.path().join(format!("full-{cap}")),
            &options(cap),
        )
        .unwrap();
        let subset = write_repack_subset(
            &plan,
            &all,
            &dir.path().join(format!("all-{cap}")),
            &options(cap),
        )
        .unwrap();
        assert_eq!(full.checksum, subset.checksum);
        assert_eq!(
            std::fs::read(&full.pack_path).unwrap(),
            std::fs::read(&subset.pack_path).unwrap()
        );
        assert_eq!((full.left_out, full.base_left_out), (0, 0));
    }
    let want = oracle(&src, 2, dir.path());
    let keep = shallow_objects(&plan, &want.roots, MAX_OBJECT, 1000).unwrap();
    let one = write_repack_subset(&plan, &keep, &dir.path().join("one"), &options(50)).unwrap();
    let two = write_repack_subset(&plan, &keep, &dir.path().join("two"), &options(50)).unwrap();
    assert_eq!(one.checksum, two.checksum);
    assert!(one.left_out > 0);
    for (a, b) in [
        (&one.pack_path, &two.pack_path),
        (&one.idx_path, &two.idx_path),
        (&one.entries_path, &two.entries_path),
    ] {
        assert_eq!(std::fs::read(a).unwrap(), std::fs::read(b).unwrap());
    }
}

fn mismatch(e: LayoutError) -> String {
    match e {
        LayoutError::Mismatch(what) => what,
        e => panic!("not a mismatch: {e}"),
    }
}

/// Refusals of the subset writer: a kept ID that no source holds is named,
/// and nothing is written.
#[test]
fn a_kept_id_in_no_source_is_refused() {
    let dir = scratch();
    let a = Obj::blob(big(1));
    let stray = Obj::blob("in no source\n");
    let s = Src::new(&build_pack(&[Spec::Full(a.clone())]), dir.path());
    let out = dir.path().join("out");
    let e =
        write_repack_subset(&sources(&[&s]), &set(&[&a, &stray]), &out, &options(50)).unwrap_err();
    assert!(mismatch(e).contains(&stray.id().to_hex()));
    let left: Vec<_> = std::fs::read_dir(&out)
        .map(|d| d.collect())
        .unwrap_or_default();
    assert!(left.is_empty(), "nothing is written");
    // An empty kept set is no pack.
    let e = write_repack_subset(&sources(&[&s]), &HashSet::new(), &out, &options(50)).unwrap_err();
    assert!(mismatch(e).contains("needs an object"));
}

/// Refusals of the shallow walk: a want whose commit is not in the
/// region, a want that is a tree or blob, a region commit that is not a
/// commit or in no source, a want peeling through more than
/// `MAX_PEELED_TAGS` tags, and a set over `max_objects`.
#[test]
fn shallow_walk_refusals() {
    let dir = scratch();
    let src = shallow_history(dir.path());
    // Tags nested 16 and 17 deep on the tip.
    git(&src, &["tag", "-a", "n1", "main", "-m", "n1"]);
    for i in 2..=MAX_PEELED_TAGS + 1 {
        let prev = format!("n{}", i - 1);
        git(&src, &["tag", "-a", &format!("n{i}"), &prev, "-m", "n"]);
    }
    let too_deep = format!("n{}", MAX_PEELED_TAGS + 1);
    let all = stored(
        &src,
        &format!("main\nside\nrefs/tags/v2\nrefs/tags/{too_deep}\n"),
        true,
        dir.path(),
    );
    let plan = sources(&[&all]);
    let rev = |r: &str| id(&git(&src, &["rev-parse", r]));
    let tip = rev("main");
    let walk = |wants: Vec<ObjectId>, commits: Vec<ObjectId>, max: usize| {
        shallow_objects(&plan, &ShallowRoots { wants, commits }, MAX_OBJECT, max)
    };

    // The depth-1 set of the tip has 9 objects: 7 from the tree walk plus
    // the commit and its tree.
    assert_eq!(walk(vec![tip], vec![tip], 1000).unwrap().len(), 9);
    match walk(vec![tip], vec![tip], 8).unwrap_err() {
        LayoutError::OverLimit(e) => assert!(e.contains("more than 8 objects"), "{e}"),
        other => panic!("expected OverLimit, got {other:?}"),
    }

    let e = mismatch(walk(vec![rev("v1")], vec![rev("main~1")], 1000).unwrap_err());
    assert!(
        e.contains("not in the region") && e.contains(&tip.to_hex()),
        "{e}"
    );

    let e = mismatch(walk(vec![rev("main^{tree}")], vec![tip], 1000).unwrap_err());
    assert!(e.contains("not a commit"), "{e}");
    let e = mismatch(walk(vec![rev("main:big.txt")], vec![tip], 1000).unwrap_err());
    assert!(e.contains("not a commit"), "{e}");

    let e = mismatch(walk(vec![], vec![rev("main^{tree}")], 1000).unwrap_err());
    assert!(e.contains("is a Tree"), "{e}");
    let absent = id("1234567890123456789012345678901234567890");
    let e = mismatch(walk(vec![], vec![absent], 1000).unwrap_err());
    assert!(e.contains("in no shallow source"), "{e}");

    let deepest = format!("n{MAX_PEELED_TAGS}");
    let got = walk(vec![rev(&deepest)], vec![tip], 1000).unwrap();
    assert_eq!(
        got.len(),
        9 + MAX_PEELED_TAGS,
        "every tag on the way is kept"
    );
    let e = mismatch(walk(vec![rev(&too_deep)], vec![tip], 1000).unwrap_err());
    assert!(e.contains("more than 16 tags"), "{e}");
}

/// Invariant: an object of the set that no source holds is refused, also
/// a blob, which is only looked up and never decoded.
#[test]
fn a_set_object_in_no_source_is_refused() {
    let dir = scratch();
    let src = shallow_history(dir.path());
    // Only the commits and trees of main: no blobs.
    let out = git_raw(
        &src,
        &[
            "pack-objects",
            "-q",
            "--revs",
            "--stdout",
            "--filter=blob:none",
        ],
        Some(b"main\n"),
    );
    assert!(out.status.success());
    let trees = Src::new(&out.stdout, dir.path());
    let tip = id(&git(&src, &["rev-parse", "main"]));
    let e = shallow_objects(
        &sources(&[&trees]),
        &ShallowRoots {
            wants: vec![tip],
            commits: vec![tip],
        },
        MAX_OBJECT,
        1000,
    )
    .unwrap_err();
    assert!(mismatch(e).contains("in no shallow source"));
}
