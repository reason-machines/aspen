//! Push ingest, against the test obligations of `docs/PLAN.md` push path
//! step 2. Every stored pack must index under stock `git index-pack`
//! without `--fix-thin`, list no ID twice, and pass [`verify_pack`].

use std::collections::BTreeSet;
use std::io::{self, Read};
use std::path::Path;

use aspen_core::error::UnpackError;
use aspen_core::id::ObjectId;
use aspen_core::ingest::{BaseLookup, InMemoryBases, ObjectKind};
use aspen_core::pack::{EntryKind, PushLimits};

use crate::ingest::{CompleteOptions, IngestedPack, complete, receive};
use crate::testutil::{Base, Obj, Spec, build_pack, git, git_index_pack, scratch};
use crate::verify::verify_pack;

const LIMITS: PushLimits = PushLimits {
    max_objects: 10_000,
    max_pack_bytes: 1 << 30,
};

fn bases(objs: &[&Obj]) -> InMemoryBases {
    let mut b = InMemoryBases::default();
    for o in objs {
        b.insert(o.id(), o.kind, o.data.clone());
    }
    b
}

/// Receives and completes `pack` against `deps`, then checks every stored
/// pack invariant on the result.
pub(crate) fn ingest(
    pack: &[u8],
    deps: &impl BaseLookup,
    dir: &Path,
) -> Result<IngestedPack, UnpackError> {
    let spool = dir.join("spool");
    let received = receive(&mut &pack[..], &spool, &LIMITS)?;
    assert_eq!(received.received_bytes, pack.len() as u64);
    let out = complete(
        &received,
        deps,
        dir,
        &CompleteOptions::new(crate::testutil::MAX_OBJECT, crate::testutil::MAX_INFLATED),
    )?;
    check_stored(&out, dir);
    Ok(out)
}

/// The stored-pack invariants, checked independently of ingest's own code
/// path: gix indexes it with no lookup (self-contained), stock git indexes
/// it without `--fix-thin`, no ID repeats in either `.idx`, every base comes
/// first, and the stored `.idx` and entry table describe it exactly.
fn check_stored(p: &IngestedPack, dir: &Path) {
    let v = verify_pack(
        &p.pack_path,
        dir,
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap_or_else(|e| panic!("stored pack: {e}"));
    let pack = std::fs::read(&p.pack_path).unwrap();
    let ids = git_index_pack(&pack, dir, false).expect("stock git indexes the stored pack");
    let unique: BTreeSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "an ID repeats in git's .idx");
    assert_eq!(ids.len(), p.object_count as usize);
    let git_idx = dir.join(format!("check-{}.idx", crate::sha1(&pack)));
    assert_eq!(
        std::fs::read(&p.idx_path).unwrap(),
        std::fs::read(git_idx).unwrap(),
        "the stored .idx is byte-identical to git index-pack's"
    );
    assert_eq!(
        std::fs::read(&p.idx_path).unwrap(),
        v.idx,
        "the stored .idx is what gix (and so git) writes for these bytes"
    );
    assert_eq!(std::fs::read(&p.entries_path).unwrap(), v.table.encode());
    assert_eq!(p.size, pack.len() as u64);
    assert_eq!(p.ref_delta_count, v.table.ref_delta_count());
    p.new_pack(aspen_core::id::RepoId::new(1).unwrap())
        .validate()
        .unwrap();
}

pub(crate) fn stored_ids(p: &IngestedPack, dir: &Path) -> BTreeSet<ObjectId> {
    verify_pack(
        &p.pack_path,
        dir,
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap()
    .table
    .entries()
    .iter()
    .map(|e| e.id)
    .collect()
}

fn blobs(n: usize) -> Vec<Obj> {
    // Each blob extends the previous one, so deltas are meaningful.
    let mut text = String::new();
    (0..n)
        .map(|i| {
            for j in 0..40 {
                text.push_str(&format!("line {i} {j}\n"));
            }
            Obj::blob(text.clone())
        })
        .collect()
}

#[test]
fn libgit2_style_all_ref_delta_with_bases_in_the_pack_is_accepted() {
    // Invariant: a candidate that `deps` lacks is not an error; gix
    // resolves it inside the pack. Every candidate misses here.
    let dir = scratch();
    let o = blobs(4);
    let pack = build_pack(&[
        Spec::Full(o[0].clone()),
        Spec::Ref(Base::Entry(0), o[1].clone()),
        Spec::Ref(Base::Entry(1), o[2].clone()),
        Spec::Ref(Base::Entry(1), o[3].clone()),
    ]);
    let unrelated = Obj::blob("unrelated");
    let out = ingest(&pack, &bases(&[&unrelated]), dir.path()).unwrap();
    assert_eq!(out.object_count, 4);
    assert!(out.completion.forward_base_check && !out.completion.reran);
    assert!(!out.repaired);
    assert_eq!(
        out.ref_delta_count, 3,
        "unanswered REF_DELTAs stay REF_DELTAs"
    );
    assert_eq!(
        stored_ids(&out, dir.path()),
        o.iter().map(Obj::id).collect()
    );
}

#[test]
fn a_resend_of_objects_deps_already_has_stores_no_duplicate() {
    // S4's dup.sh: an all-REF_DELTA resend against bases `deps` holds,
    // including a REF_DELTA against a deltified object of the pack (o[2]
    // is based on o[1], itself a delta). The first run inserts copies of
    // o[0] and o[1]; the rerun refuses exactly those, and nothing repeats.
    let dir = scratch();
    let o = blobs(3);
    let pack = build_pack(&[
        Spec::Full(o[0].clone()),
        Spec::Ref(Base::Entry(0), o[1].clone()),
        Spec::Ref(Base::Entry(1), o[2].clone()),
    ]);
    let deps = bases(&[&o[0], &o[1], &o[2]]);
    let out = ingest(&pack, &deps, dir.path()).unwrap();
    assert!(out.completion.reran, "the first run stored duplicates");
    assert!(out.completion.forward_base_check);
    assert_eq!(out.object_count, 3);
    assert!(
        out.completion_bases.is_empty(),
        "every inserted copy was refused"
    );
}

#[test]
fn a_forward_ref_delta_without_the_base_in_deps_is_stored_base_first() {
    // Invariant: forward bases are repaired, never rejected (S1). gix leaves
    // the REF_DELTA before its base; the repair moves it after the base.
    let dir = scratch();
    let o = blobs(2);
    let pack = build_pack(&[
        Spec::Ref(Base::Entry(1), o[1].clone()),
        Spec::Full(o[0].clone()),
    ]);
    let out = ingest(&pack, &InMemoryBases::default(), dir.path()).unwrap();
    assert!(out.repaired && !out.completion.reran);
    let table = verify_pack(
        &out.pack_path,
        dir.path(),
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap()
    .table;
    assert_eq!(
        table.entries().iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![o[0].id(), o[1].id()]
    );
}

#[test]
fn a_forward_ref_delta_with_the_base_also_in_deps_is_stored_base_first_once() {
    // The lookup answers the base, so gix inserts a copy before the delta
    // while the client's own copy follows: a repeat. The rerun refuses it,
    // which leaves the REF_DELTA pointing forward, and the repair fixes
    // that. Both rare paths in one push.
    let dir = scratch();
    let o = blobs(2);
    let pack = build_pack(&[
        Spec::Ref(Base::Entry(1), o[1].clone()),
        Spec::Full(o[0].clone()),
    ]);
    let out = ingest(&pack, &bases(&[&o[0]]), dir.path()).unwrap();
    assert!(out.completion.reran && out.repaired);
    assert_eq!(out.object_count, 2);
}

#[test]
fn repair_moves_dependents_of_a_forward_delta_with_it_and_rewrites_distances() {
    // b is a forward REF_DELTA on a; c is an OFS_DELTA on b; d is unrelated
    // and keeps its place. Expected order: d, a, b, c (b and c move to
    // just after a, depth first), with c's OFS distance recomputed.
    let dir = scratch();
    let o = blobs(3);
    let d = Obj::blob("unrelated and big ".repeat(50));
    let pack = build_pack(&[
        Spec::Ref(Base::Entry(3), o[1].clone()),
        Spec::Ofs(0, o[2].clone()),
        Spec::Full(d.clone()),
        Spec::Full(o[0].clone()),
    ]);
    let out = ingest(&pack, &InMemoryBases::default(), dir.path()).unwrap();
    assert!(out.repaired);
    let table = verify_pack(
        &out.pack_path,
        dir.path(),
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap()
    .table;
    let ids: Vec<_> = table.entries().iter().map(|e| e.id).collect();
    assert_eq!(ids, vec![d.id(), o[0].id(), o[1].id(), o[2].id()]);
    let c = table.entries()[3];
    assert_eq!(
        c.kind,
        EntryKind::OfsDelta {
            base_offset: table.entries()[2].offset
        }
    );
}

#[test]
fn a_ref_delta_whose_base_is_nowhere_fails_with_unpack_error() {
    let dir = scratch();
    let o = blobs(2);
    let pack = build_pack(&[Spec::Ref(Base::External(o[0].clone()), o[1].clone())]);
    let spool = dir.path().join("spool");
    let received = receive(&mut &pack[..], &spool, &LIMITS).unwrap();
    let err = complete(
        &received,
        &InMemoryBases::default(),
        dir.path(),
        &CompleteOptions::new(crate::testutil::MAX_OBJECT, crate::testutil::MAX_INFLATED),
    )
    .unwrap_err();
    assert_eq!(err, UnpackError::MissingBase { id: o[0].id() });
    assert!(err.to_string().contains(&o[0].id().to_hex()));
    assert_no_outputs(dir.path());
}

#[test]
fn a_thin_pack_is_completed_from_deps() {
    // The ordinary stock-git case: the lookup answers every candidate, so
    // no REF_DELTA remains and the forward-base check is skipped.
    let dir = scratch();
    let o = blobs(2);
    let pack = build_pack(&[Spec::Ref(Base::External(o[0].clone()), o[1].clone())]);
    let out = ingest(&pack, &bases(&[&o[0]]), dir.path()).unwrap();
    assert_eq!(out.object_count, 2);
    assert_eq!(out.ref_delta_count, 0);
    assert!(!out.completion.forward_base_check && !out.repaired);
    assert_eq!(out.completion_bases.as_slice(), &[o[0].id()]);
}

#[test]
fn a_pack_that_holds_an_object_twice_is_refused_naming_it() {
    // The client's own repeat survives the rerun: unpack error naming the
    // object, and no outputs (so no `packs` row can be written).
    let dir = scratch();
    let o = blobs(2);
    for specs in [
        vec![Spec::Full(o[0].clone()), Spec::Full(o[0].clone())],
        vec![
            Spec::Full(o[0].clone()),
            Spec::Ref(Base::Entry(0), o[1].clone()),
            Spec::Full(o[1].clone()),
        ],
    ] {
        let pack = build_pack(&specs);
        let spool = dir.path().join("spool");
        let received = receive(&mut &pack[..], &spool, &LIMITS).unwrap();
        let err = complete(
            &received,
            &bases(&[&o[0]]),
            dir.path(),
            &CompleteOptions::new(crate::testutil::MAX_OBJECT, crate::testutil::MAX_INFLATED),
        )
        .unwrap_err();
        let id = match err {
            UnpackError::DuplicateObject { id } => id,
            other => panic!("expected DuplicateObject, got {other:?}"),
        };
        assert!(id == o[0].id() || id == o[1].id());
        assert!(err.to_string().contains(&id.to_hex()));
        assert_no_outputs(dir.path());
    }
}

fn assert_no_outputs(dir: &Path) {
    for e in std::fs::read_dir(dir).unwrap() {
        let name = e.unwrap().file_name().into_string().unwrap();
        assert!(
            !(name.ends_with(".pack")
                || name.ends_with(".idx")
                || name.ends_with(".entries")
                || name.ends_with(".tmp")),
            "left {name} behind"
        );
    }
}

/// A reader that fails if anything past the first `allowed` bytes is read.
struct Fence<'a> {
    data: &'a [u8],
    pos: usize,
    allowed: usize,
}

impl Read for Fence<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        assert!(self.pos < self.allowed, "read past the header");
        let n = buf
            .len()
            .min(self.allowed - self.pos)
            .min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[test]
fn the_header_object_count_is_checked_before_the_body_is_read() {
    let dir = scratch();
    let pack = build_pack(&blobs(3).into_iter().map(Spec::Full).collect::<Vec<_>>());
    let limits = PushLimits {
        max_objects: 2,
        max_pack_bytes: 1 << 20,
    };
    let mut r = Fence {
        data: &pack,
        pos: 0,
        allowed: 12,
    };
    let err = receive(&mut r, &dir.path().join("spool"), &limits).unwrap_err();
    assert_eq!(err, UnpackError::TooManyObjects { count: 3, limit: 2 });
}

#[test]
fn a_pack_over_the_size_limit_is_refused() {
    let dir = scratch();
    let pack = build_pack(&blobs(3).into_iter().map(Spec::Full).collect::<Vec<_>>());
    let spool = dir.path().join("spool");
    let exact = PushLimits {
        max_objects: 10,
        max_pack_bytes: pack.len() as u64,
    };
    receive(&mut &pack[..], &spool, &exact).expect("a pack of exactly the limit is fine");
    let under = PushLimits {
        max_pack_bytes: pack.len() as u64 - 1,
        ..exact
    };
    assert_eq!(
        receive(&mut &pack[..], &spool, &under).unwrap_err(),
        UnpackError::PackTooLarge {
            limit: pack.len() as u64 - 1
        }
    );
}

#[test]
fn malformed_packs_are_refused_in_pass_1() {
    let dir = scratch();
    let spool = dir.path().join("spool");
    let pack = build_pack(&blobs(2).into_iter().map(Spec::Full).collect::<Vec<_>>());
    let mut bad_trailer = pack.clone();
    *bad_trailer.last_mut().unwrap() ^= 1;
    let mut bad_data = pack.clone();
    bad_data[20] ^= 0xff;
    let mut extra = pack.clone();
    extra.push(0);
    for (what, bytes) in [
        ("trailer", &bad_trailer[..]),
        ("data", &bad_data[..]),
        ("truncated", &pack[..pack.len() - 5]),
        ("header only", &pack[..12]),
        ("not a pack", b"HELLO, WORLD"),
        ("trailing bytes", &extra[..]),
    ] {
        match receive(&mut &bytes[..], &spool, &LIMITS) {
            Err(UnpackError::Malformed { .. }) => {}
            other => panic!("{what}: expected Malformed, got {other:?}"),
        }
    }
}

#[test]
fn an_empty_pack_is_received_but_stores_nothing() {
    let dir = scratch();
    let pack = build_pack(&[]);
    let received = receive(&mut &pack[..], &dir.path().join("spool"), &LIMITS).unwrap();
    assert_eq!(received.object_count, 0);
    assert!(
        complete(
            &received,
            &InMemoryBases::default(),
            dir.path(),
            &CompleteOptions::new(crate::testutil::MAX_OBJECT, crate::testutil::MAX_INFLATED)
        )
        .is_err()
    );
}

#[test]
fn candidates_are_the_ref_delta_bases() {
    let dir = scratch();
    let o = blobs(3);
    let x = Obj::blob("external");
    let pack = build_pack(&[
        Spec::Full(o[0].clone()),
        Spec::Ref(Base::Entry(0), o[1].clone()),
        Spec::Ofs(1, o[2].clone()),
        Spec::Ref(Base::External(x.clone()), Obj::blob("external, edited")),
    ]);
    let r = receive(&mut &pack[..], &dir.path().join("spool"), &LIMITS).unwrap();
    let mut want = [o[0].id(), x.id()];
    want.sort();
    assert_eq!(r.candidates.0.as_slice(), &want[..]);
}

/// Every object of a repository, as prefetched bases.
fn all_objects(repo: &Path) -> InMemoryBases {
    let mut b = InMemoryBases::default();
    let list = git(
        repo,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype)",
        ],
    );
    for line in list.lines() {
        let (id, kind) = line.split_once(' ').unwrap();
        let kind = match kind {
            "commit" => ObjectKind::Commit,
            "tree" => ObjectKind::Tree,
            "blob" => ObjectKind::Blob,
            _ => ObjectKind::Tag,
        };
        let data = crate::testutil::git_raw(repo, &["cat-file", kind_name(kind), id], None).stdout;
        b.insert(ObjectId::parse_hex(id).unwrap(), kind, data);
    }
    b
}

fn kind_name(k: ObjectKind) -> &'static str {
    match k {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    }
}

/// A history of `n` commits to one growing file plus a renamed copy.
pub(crate) fn history(dir: &Path, n: usize) -> std::path::PathBuf {
    let src = dir.join("src");
    std::fs::create_dir_all(&src).unwrap();
    git(&src, &["init", "-q", "-b", "main"]);
    // Each commit edits lines in place (and adds one), so stock git
    // deltifies new versions against old ones, including thin bases.
    let mut lines: Vec<String> = (0..400).map(|j| format!("line {j}\n")).collect();
    for i in 1..=n {
        lines[(i * 37) % 400] = format!("edited in commit {i}\n");
        lines.push(format!("added in commit {i}\n"));
        let text = lines.concat();
        std::fs::write(src.join("f.txt"), &text).unwrap();
        std::fs::write(src.join(format!("g{}.txt", i % 3)), &text).unwrap();
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-q", "-m", &format!("commit {i}")]);
    }
    src
}

fn pack_objects(repo: &Path, revs: &str, flags: &[&str]) -> Vec<u8> {
    let mut args = vec!["pack-objects", "-q", "--revs", "--stdout"];
    args.extend_from_slice(flags);
    let out = crate::testutil::git_raw(repo, &args, Some(revs.as_bytes()));
    assert!(out.status.success());
    out.stdout
}

#[test]
fn stock_git_thin_push_against_real_deps() {
    // A stock `git push` pack (OFS_DELTA, thin) against a base repository
    // at HEAD~10: completion fills every thin base, nothing repeats, and
    // the scan finds exactly the commit the push builds on as a missing
    // parent.
    let dir = scratch();
    let src = history(dir.path(), 20);
    let base_repo = dir.path().join("base.git");
    git(
        dir.path(),
        &[
            "clone",
            "-q",
            "--bare",
            "--no-local",
            src.to_str().unwrap(),
            base_repo.to_str().unwrap(),
        ],
    );
    let old = git(&src, &["rev-parse", "HEAD~10"]).trim().to_owned();
    git(&base_repo, &["update-ref", "refs/heads/main", &old]);
    git(&base_repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&base_repo, &["gc", "-q", "--prune=now"]);
    let deps = all_objects(&base_repo);
    let pack = pack_objects(&src, "HEAD\n^HEAD~10\n", &["--thin", "--delta-base-offset"]);
    let out = ingest(&pack, &deps, dir.path()).unwrap();
    assert!(!out.completion.reran && !out.repaired);
    assert!(!out.completion_bases.is_empty(), "a thin push needs bases");
    let mut buf = Vec::new();
    for id in out.completion_bases.as_slice() {
        assert!(deps.find_base(id, &mut buf).unwrap().is_some());
    }
    let scan = crate::scan::scan(
        &out.pack_path,
        &out.idx_path,
        crate::scan::ScanOptions {
            commits: true,
            missing: true,
            thread_limit: Some(2),
            max_object_bytes: crate::testutil::MAX_OBJECT,
            max_inflated_bytes: crate::testutil::MAX_INFLATED,
            verify_checksums: false,
        },
    )
    .unwrap();
    assert_eq!(scan.commits.len(), 10);
    let old_id = ObjectId::parse_hex(&old).unwrap();
    assert!(scan.missing.contains(&old_id));
    // Everything missing is in deps: the connectivity check would pass.
    for id in scan.missing.as_slice() {
        assert!(deps.find_base(id, &mut buf).unwrap().is_some(), "{id}");
    }
}

#[test]
fn stock_git_all_ref_delta_full_resend_stores_no_duplicate() {
    // S4's dup.sh with stock git: the whole history written all-REF_DELTA
    // (no --delta-base-offset, as libgit2 writes deltas) against deps
    // holding the first 20 commits. The stored pack is closed, so
    // `index-pack --strict` (which rejects a repeat) must pass too.
    let dir = scratch();
    let src = history(dir.path(), 30);
    let base_repo = dir.path().join("base.git");
    git(
        dir.path(),
        &[
            "clone",
            "-q",
            "--bare",
            "--no-local",
            src.to_str().unwrap(),
            base_repo.to_str().unwrap(),
        ],
    );
    let old = git(&src, &["rev-parse", "HEAD~10"]).trim().to_owned();
    git(&base_repo, &["update-ref", "refs/heads/main", &old]);
    git(&base_repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&base_repo, &["gc", "-q", "--prune=now"]);
    let deps = all_objects(&base_repo);
    let pack = pack_objects(&src, "HEAD\n", &[]);
    let out = ingest(&pack, &deps, dir.path()).unwrap();
    assert!(
        out.completion.reran,
        "the resend made completion insert copies"
    );
    let total = git(&src, &["rev-list", "--objects", "--count", "HEAD"]);
    assert_eq!(out.object_count.to_string(), total.trim());
    let stored = std::fs::read(&out.pack_path).unwrap();
    git_index_pack(&stored, dir.path(), true).expect("--strict accepts the stored pack");
}

/// A pack of a full blob and an OFS_DELTA against it whose delta header
/// claims `result_len` bytes, with no instructions: tiny on the wire.
/// `fillers` tiny blobs, then the blob `base`, then an OFS_DELTA on it with
/// no instructions whose header claims `base_claim` and `result_len`.
pub(crate) fn pack_with_claimed_delta(
    fillers: usize,
    base: &[u8],
    base_claim: u64,
    result_len: u64,
) -> Vec<u8> {
    fn varint(mut n: u64, out: &mut Vec<u8>) {
        loop {
            let b = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }
    let mut delta = Vec::new();
    varint(base_claim, &mut delta);
    varint(result_len, &mut delta);
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&(fillers as u32 + 2).to_be_bytes());
    for i in 0..fillers {
        let data = format!("f{i}");
        crate::header::encode_type_and_size(3, data.len() as u64, &mut out);
        out.extend(crate::testutil::deflate(data.as_bytes()));
    }
    let first = out.len() as u64;
    crate::header::encode_type_and_size(3, base.len() as u64, &mut out);
    out.extend(crate::testutil::deflate(base));
    let second = out.len() as u64;
    crate::header::encode_type_and_size(6, delta.len() as u64, &mut out);
    crate::header::encode_distance(second - first, &mut out);
    out.extend(crate::testutil::deflate(&delta));
    let sum = crate::sha1(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

/// `fillers` tiny blobs, then (unless `thin`) the blob `base`, then a
/// REF_DELTA on `base`'s ID with no instructions whose header claims
/// `base_claim` and `result_len`. With `thin`, the base is not in the pack.
pub(crate) fn pack_with_claimed_ref_delta(
    fillers: usize,
    base: &[u8],
    thin: bool,
    base_claim: u64,
    result_len: u64,
) -> Vec<u8> {
    let mut delta = Vec::new();
    for n in [base_claim, result_len] {
        let mut n = n;
        loop {
            let b = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                delta.push(b);
                break;
            }
            delta.push(b | 0x80);
        }
    }
    let entries = fillers as u32 + if thin { 1 } else { 2 };
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&entries.to_be_bytes());
    for i in 0..fillers {
        let data = format!("f{i}");
        crate::header::encode_type_and_size(3, data.len() as u64, &mut out);
        out.extend(crate::testutil::deflate(data.as_bytes()));
    }
    if !thin {
        crate::header::encode_type_and_size(3, base.len() as u64, &mut out);
        out.extend(crate::testutil::deflate(base));
    }
    crate::header::encode_type_and_size(7, delta.len() as u64, &mut out);
    out.extend_from_slice(Obj::blob(base).id().as_bytes());
    out.extend(crate::testutil::deflate(&delta));
    let sum = crate::sha1(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

fn pack_with_claimed_delta_result(base: &[u8], result_len: u64) -> Vec<u8> {
    pack_with_claimed_delta(0, base, base.len() as u64, result_len)
}

#[test]
fn a_delta_claiming_a_huge_result_is_refused_before_it_is_allocated() {
    // Invariant: no allocation sized from bytes the client declares exceeds
    // the configured object limit. A 30-byte pack whose delta claims a
    // 2^40-byte result is refused as malformed (gix would otherwise reserve
    // and zero 1 TiB per resolver thread).
    let dir = scratch();
    let pack = pack_with_claimed_delta_result(b"base", 1 << 40);
    assert!(pack.len() < 64);
    let spool = dir.path().join("spool");
    let received = receive(&mut &pack[..], &spool, &LIMITS).unwrap();
    let small = crate::MaxObjectBytes::new(1 << 20).unwrap();
    let err = complete(
        &received,
        &InMemoryBases::default(),
        dir.path(),
        &CompleteOptions::new(small, crate::testutil::MAX_INFLATED),
    )
    .unwrap_err();
    assert!(
        matches!(&err, UnpackError::Malformed { reason } if reason.contains("object limit")),
        "{err:?}"
    );
    assert_no_outputs(dir.path());
}

#[test]
fn a_large_push_fits_the_tree_allowance() {
    // Invariant: the object limit bounds object sizes only; gix's per-entry
    // allocations (its delta tree) get DELTA_TREE_BYTES_PER_ENTRY per entry,
    // so a push of many small objects under a tiny object limit completes.
    // Fails if a gix upgrade grows its tree item past the allowance.
    let dir = scratch();
    let objs: Vec<Obj> = (0..3000).map(|i| Obj::blob(format!("b{i}"))).collect();
    let specs: Vec<Spec> = objs.iter().cloned().map(Spec::Full).collect();
    let pack = build_pack(&specs);
    let spool = dir.path().join("spool");
    let received = receive(&mut &pack[..], &spool, &LIMITS).unwrap();
    let tiny = crate::MaxObjectBytes::new(64).unwrap();
    let out = complete(
        &received,
        &InMemoryBases::default(),
        dir.path(),
        &CompleteOptions::new(tiny, crate::testutil::MAX_INFLATED),
    )
    .unwrap();
    assert_eq!(out.object_count, 3000);
}

/// Receives `pack` and completes it under a `limit`-byte object limit.
fn complete_under(pack: &[u8], limit: u64, dir: &Path) -> Result<IngestedPack, UnpackError> {
    let received = receive(&mut &pack[..], &dir.join("spool"), &LIMITS).unwrap();
    complete(
        &received,
        &InMemoryBases::default(),
        dir,
        &CompleteOptions::new(
            crate::MaxObjectBytes::new(limit).unwrap(),
            crate::testutil::MAX_INFLATED,
        ),
    )
}

fn assert_over_limit(err: &UnpackError) {
    assert!(
        matches!(err, UnpackError::Malformed { reason } if reason.contains("object limit")),
        "{err:?}"
    );
}

#[test]
fn a_delta_result_over_the_limit_is_refused_in_a_large_push() {
    // Invariant: every size a pushed pack declares (entry, delta base,
    // delta result) is checked against the object limit itself, whatever
    // the push's object count. gix's own limit also covers its delta tree
    // (256 B per entry), so on its own it would let this 1 MiB claim
    // through a 3,000-object push under a 64-byte limit.
    let dir = scratch();
    let pack = pack_with_claimed_delta(3000, b"base", 4, 1 << 20);
    let err = complete_under(&pack, 64, dir.path()).unwrap_err();
    assert_over_limit(&err);
    assert_no_outputs(dir.path());
}

#[test]
fn a_delta_base_size_over_the_limit_is_refused() {
    // Same invariant, for the base size a delta declares.
    let dir = scratch();
    let pack = pack_with_claimed_delta(3000, b"base", 1 << 20, 4);
    let err = complete_under(&pack, 64, dir.path()).unwrap_err();
    assert_over_limit(&err);
    assert_no_outputs(dir.path());
}

#[test]
fn an_entry_over_the_limit_is_refused_in_a_large_push() {
    // Same invariant, for an entry's own declared size: one real 200-byte
    // blob among 3,000 tiny ones, under a 64-byte limit.
    let dir = scratch();
    let mut specs: Vec<Spec> = (0..3000)
        .map(|i| Spec::Full(Obj::blob(format!("b{i}"))))
        .collect();
    specs.push(Spec::Full(Obj::blob(vec![b'x'; 200])));
    let pack = build_pack(&specs);
    let err = complete_under(&pack, 64, dir.path()).unwrap_err();
    assert_over_limit(&err);
    assert_no_outputs(dir.path());
}

#[test]
fn verify_pack_refuses_sizes_over_the_limit() {
    // Invariant: verify_pack checks declared sizes itself too, so it is safe
    // on untrusted input whatever the input's size.
    let dir = scratch();
    let tiny = crate::MaxObjectBytes::new(64).unwrap();
    for pack in [
        pack_with_claimed_delta(3000, b"base", 4, 1 << 20),
        pack_with_claimed_delta(3000, b"base", 1 << 20, 4),
    ] {
        let path = dir.path().join("in.pack");
        std::fs::write(&path, &pack).unwrap();
        let err = verify_pack(&path, dir.path(), tiny, crate::testutil::MAX_INFLATED).unwrap_err();
        assert!(
            matches!(&err, crate::verify::VerifyError::OverLimit(_)),
            "{err:?}"
        );
    }
    // The same layout within the limit verifies: 3000 tiny blobs, the blob
    // `base` and an OFS_DELTA on it, here a valid one (copy the base, insert
    // a byte). A delta with no instructions and an empty result would be 2
    // bytes, under the 4 Git requires (issue #162), so it is refused.
    let mut specs: Vec<Spec> = (0..3000)
        .map(|i| Spec::Full(Obj::blob(format!("f{i}"))))
        .collect();
    specs.push(Spec::Full(Obj::blob("base")));
    specs.push(Spec::Ofs(3000, Obj::blob("based")));
    let ok = build_pack(&specs);
    let path = dir.path().join("ok.pack");
    std::fs::write(&path, &ok).unwrap();
    verify_pack(&path, dir.path(), tiny, crate::testutil::MAX_INFLATED).unwrap();
}

#[test]
fn ref_delta_sizes_over_the_limit_are_refused() {
    // Invariant: the declared-size check covers REF_DELTA headers too, both
    // with the base in the pack and with a thin base (checked before any
    // lookup), for the base size and the result size.
    for thin in [false, true] {
        for (base_claim, result) in [(4, 1 << 20), (1 << 20, 4)] {
            let dir = scratch();
            let pack = pack_with_claimed_ref_delta(3000, b"base", thin, base_claim, result);
            let err = complete_under(&pack, 64, dir.path()).unwrap_err();
            assert_over_limit(&err);
            assert_no_outputs(dir.path());
        }
    }
}

/// Receives `pack` and completes it against `deps` under a 64 KiB object
/// limit and a `total`-byte total limit, on one thread.
fn complete_with_total(
    pack: &[u8],
    deps: &impl BaseLookup,
    total: u64,
    dir: &Path,
) -> Result<IngestedPack, UnpackError> {
    let received = receive(&mut &pack[..], &dir.join("spool"), &LIMITS).unwrap();
    let mut options = CompleteOptions::new(
        crate::MaxObjectBytes::new(64 << 10).unwrap(),
        crate::MaxInflatedBytes::new(total).unwrap(),
    );
    options.thread_limit = Some(1);
    complete(&received, deps, dir, &options)
}

fn assert_over_total(err: &UnpackError, with_bases: bool) {
    assert!(
        matches!(err, UnpackError::Malformed { reason }
            if reason.contains("limit for one pack")
                && reason.contains("delta bases completion inserts") == with_bases),
        "{err:?}"
    );
}

/// 16 levels of 64 KiB (see `ladder_pack`): about 1.1 MB declared, under
/// 10 KB on the wire.
const LADDER: (usize, usize) = (16, 64 << 10);

#[test]
fn a_pack_inflating_past_the_total_limit_is_refused_before_gix() {
    // Invariant: the sum of everything a pack declares it inflates to is
    // checked against the total limit before gix resolves it, since gix's
    // resolver can hold every object on a path of the delta tree at once:
    // this pack, every size within the object limit and depth 16, would
    // hold all 16 levels. Measured at 8 MiB levels: 64 levels, 84 KB on the
    // wire, held 521 MiB on one thread (`ladder_peak_memory`).
    let pack = crate::testutil::ladder_pack(LADDER.0, LADDER.1);
    assert!(pack.len() < 10_000, "{}", pack.len());
    let dir = scratch();
    let err =
        complete_with_total(&pack, &InMemoryBases::default(), 512 << 10, dir.path()).unwrap_err();
    assert_over_total(&err, false);
    assert_no_outputs(dir.path());
    // The same pack, under a total that holds it, is stored.
    let dir = scratch();
    let out = complete_with_total(&pack, &InMemoryBases::default(), 2 << 20, dir.path()).unwrap();
    assert_eq!(out.object_count as usize, 1 + 2 * LADDER.0);
    check_stored(&out, dir.path());
}

#[test]
fn thin_bases_count_toward_the_total_limit() {
    // Invariant: the bases completion inserts count toward the total too.
    // The pack declares 40 KiB + 1 (one delta result); its thin base adds
    // 40 KiB, which a 60 KiB total does not hold and a 120 KiB one does.
    let base = Obj::blob(vec![b'x'; 40 << 10]);
    let mut data = base.data.clone();
    data.push(b'y');
    let pack = build_pack(&[Spec::Ref(Base::External(base.clone()), Obj::blob(data))]);
    let deps = bases(&[&base]);
    let dir = scratch();
    let err = complete_with_total(&pack, &deps, 60 << 10, dir.path()).unwrap_err();
    assert_over_total(&err, true);
    assert_no_outputs(dir.path());
    let dir = scratch();
    let out = complete_with_total(&pack, &deps, 120 << 10, dir.path()).unwrap();
    assert_eq!(out.completion_bases.len(), 1);
}

#[test]
fn an_inserted_base_over_the_object_limit_is_refused() {
    // Invariant: each base completion inserts is within the object limit,
    // whatever size the delta declares for it. The delta declares the 40-byte
    // base it was built against; the lookup answers that ID with 128 KiB,
    // over the 64 KiB object limit, under a total that holds it.
    let base = Obj::blob(vec![b'x'; 40]);
    let mut data = base.data.clone();
    data.push(b'y');
    let pack = build_pack(&[Spec::Ref(Base::External(base.clone()), Obj::blob(data))]);
    let mut deps = InMemoryBases::default();
    deps.insert(base.id(), base.kind, vec![b'x'; 128 << 10]);
    let dir = scratch();
    let err = complete_with_total(&pack, &deps, 1 << 30, dir.path()).unwrap_err();
    assert!(
        matches!(&err, UnpackError::Malformed { reason }
            if reason.contains("delta base") && reason.contains("65536-byte object limit")),
        "{err:?}"
    );
    assert_no_outputs(dir.path());
}

#[test]
fn verify_pack_refuses_a_pack_inflating_past_the_total_limit() {
    // Same invariant for verify_pack's own walk.
    let dir = scratch();
    let path = dir.path().join("ladder.pack");
    std::fs::write(&path, crate::testutil::ladder_pack(LADDER.0, LADDER.1)).unwrap();
    let object = crate::MaxObjectBytes::new(64 << 10).unwrap();
    let total = |n| crate::MaxInflatedBytes::new(n).unwrap();
    let err = verify_pack(&path, dir.path(), object, total(512 << 10)).unwrap_err();
    assert!(
        matches!(&err, crate::verify::VerifyError::OverLimit(m) if m.contains("limit for one pack")),
        "{err:?}"
    );
    let v = verify_pack(&path, dir.path(), object, total(2 << 20)).unwrap();
    assert_eq!(v.max_delta_depth as usize, LADDER.0);
}

#[test]
fn verify_pack_stops_an_entry_that_inflates_past_its_declared_size() {
    // Invariant: verify_pack's walk inflates no entry past its declared
    // size (plus one byte to detect the overrun), so a zlib bomb costs what
    // it claims, not what it expands to. The blob claims 4 bytes and
    // inflates to 1 MiB.
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&1u32.to_be_bytes());
    crate::header::encode_type_and_size(3, 4, &mut out);
    out.extend(crate::testutil::deflate(&vec![0u8; 1 << 20]));
    let sum = crate::sha1(&out);
    out.extend_from_slice(sum.as_bytes());
    let dir = scratch();
    let path = dir.path().join("bomb.pack");
    std::fs::write(&path, &out).unwrap();
    let err = verify_pack(
        &path,
        dir.path(),
        crate::testutil::MAX_OBJECT,
        crate::testutil::MAX_INFLATED,
    )
    .unwrap_err();
    assert!(
        matches!(&err, crate::verify::VerifyError::Index(m) if m.contains("inflates past its declared size of 4 bytes")),
        "{err:?}"
    );
}

/// `n + 1` blobs, each a delta of the one before: a chain of depth `n`.
fn chain_pack(n: usize) -> Vec<u8> {
    let objs: Vec<Obj> = (0..=n)
        .map(|i| Obj::blob(format!("object {i:06}\n")))
        .collect();
    let specs: Vec<Spec> = objs
        .iter()
        .enumerate()
        .map(|(i, o)| {
            if i == 0 {
                Spec::Full(o.clone())
            } else {
                Spec::Ofs(i - 1, o.clone())
            }
        })
        .collect();
    build_pack(&specs)
}

#[test]
fn a_delta_chain_deeper_than_git_allows_is_refused() {
    // Invariant: no stored pack has a delta chain deeper than Git's maximum
    // (4095), and the depth of every stored pack is known at ingest. Git's
    // pack-objects never writes a deeper chain; a client that does would
    // make every later read and stitch of the pack walk it.
    let dir = scratch();
    let at_max = ingest(
        &chain_pack(crate::MAX_DELTA_DEPTH as usize),
        &InMemoryBases::default(),
        &dir.path().join("a").tap_mkdir(),
    )
    .unwrap();
    assert_eq!(at_max.max_delta_depth, crate::MAX_DELTA_DEPTH);
    let over = dir.path().join("b").tap_mkdir();
    let pack = chain_pack(crate::MAX_DELTA_DEPTH as usize + 1);
    let received = receive(&mut &pack[..], &over.join("spool"), &LIMITS).unwrap();
    let err = complete(
        &received,
        &InMemoryBases::default(),
        &over,
        &CompleteOptions::new(crate::testutil::MAX_OBJECT, crate::testutil::MAX_INFLATED),
    )
    .unwrap_err();
    assert!(
        matches!(&err, UnpackError::Malformed { reason } if reason.contains("depth 4096")),
        "{err:?}"
    );
    assert_no_outputs(&over);
}

trait TapMkdir {
    fn tap_mkdir(self) -> Self;
}

impl TapMkdir for std::path::PathBuf {
    fn tap_mkdir(self) -> Self {
        std::fs::create_dir_all(&self).unwrap();
        self
    }
}

/// Peak resident memory of this process (`VmHWM`), in bytes.
fn peak_rss() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    let kb: u64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    kb * 1024
}

fn complete_with_object_limit(
    received: &crate::ingest::Received,
    object: usize,
    total: u64,
    dir: &Path,
) -> Result<IngestedPack, UnpackError> {
    let mut options = CompleteOptions::new(
        crate::MaxObjectBytes::new(object as u64).unwrap(),
        crate::MaxInflatedBytes::new(total).unwrap(),
    );
    options.thread_limit = Some(1);
    complete(received, &InMemoryBases::default(), dir, &options)
}

/// Measures what completing a `ladder_pack` of 8 MiB levels (count from
/// `LADDER_LEVELS`, default 32) adds to peak memory on one thread, under a
/// total limit that holds it. Before the total limit this shape was
/// accepted under an 8 MiB object limit and held one level per level
/// (decisions log, A2); a total below levels × 8 MiB now refuses it first.
#[test]
#[ignore = "measurement: run alone with --ignored --test-threads=1 --nocapture"]
fn ladder_peak_memory() {
    let dir = scratch();
    let levels: usize = std::env::var("LADDER_LEVELS").map_or(32, |v| v.parse().unwrap());
    let size = 8usize << 20;
    let pack = crate::testutil::ladder_pack(levels, size);
    let received = receive(&mut &pack[..], &dir.path().join("spool"), &LIMITS).unwrap();
    drop(pack);
    // The rule this measures: a total below what the ladder declares
    // refuses it before gix resolves anything.
    let small = complete_with_object_limit(&received, size, (size as u64) * 2, dir.path());
    assert!(
        matches!(&small, Err(UnpackError::Malformed { reason }) if reason.contains("limit for one pack")),
        "{small:?}"
    );
    assert_no_outputs(dir.path());
    let before = peak_rss();
    let mut options = CompleteOptions::new(
        crate::MaxObjectBytes::new(size as u64).unwrap(),
        crate::testutil::MAX_INFLATED,
    );
    options.thread_limit = Some(1);
    let out = complete(&received, &InMemoryBases::default(), dir.path(), &options);
    let after = peak_rss();
    eprintln!(
        "ladder {levels} x {size} B: wire {} B, result {:?}, peak RSS {} MiB -> {} MiB (+{} MiB)",
        received.received_bytes,
        out.as_ref().map(|o| o.object_count),
        before >> 20,
        after >> 20,
        (after - before) >> 20
    );
}
